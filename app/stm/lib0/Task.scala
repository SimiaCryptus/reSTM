package stm.lib0

import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm
import storage.Restm.PointerType
import storage.data.KryoValue

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object Task {
  def create[T](f: (Restm, ExecutionContext) => TaskResult[T], ancestors: List[Task[_]] = List.empty)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    STMPtr.dynamic[TaskData[T]](new TaskData[T](Option(KryoValue(f)), triggers = ancestors)).map(new Task(_))

  def static[T](id: PointerType) = new Task(STMPtr.static[TaskData[T]](id, new TaskData[T]()))

  sealed trait TaskResult[T]
  final case class TaskSuccess[T](value:T) extends TaskResult[T]
  final case class TaskContinue[T](queue: StmExecutionQueue, newFunction: (Restm, ExecutionContext) => TaskResult[T], newTriggers:List[Task[T]] = List.empty) extends TaskResult[T]

  val scheduledThreadPool: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
}

import stm.lib0.Task._

class Task[T](root : STMPtr[TaskData[T]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase {
    def result() = atomic { Task.this.result()(_,executionContext) }
    def isComplete() = atomic { Task.this.isComplete()(_,executionContext) }
    def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U]) = atomic { Task.this.map(queue, function)(_,executionContext) }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def result() = sync { AtomicApi.this.result() }
      def isComplete() = sync { AtomicApi.this.isComplete() }
      def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U]) = sync { AtomicApi.this.map(queue, function) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def result()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.result() }
    def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.map(queue, function) }
    def isComplete()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.isComplete() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Task[U]] = {
    val func: (Restm, ExecutionContext) => TaskResult[U] = wrapMap(function)
    Task.create(func, List(Task.this)).flatMap(task=>task.initTriggers(queue).map(_=>task))
  }

  def wrapMap[U](function: (T, Restm, ExecutionContext) => U): (Restm, ExecutionContext) => U = {
    (c, e) => function(Task.this.atomic(c, e).sync.result(), c, e)
  }

  def isComplete()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.read().map(currentState => currentState.result.isDefined || currentState.exception.isDefined)
  }

  def future(implicit cluster: Restm, executionContext: ExecutionContext) = {
    val promise = Promise[T]()
    val schedule: ScheduledFuture[_] = scheduledThreadPool.schedule(new Runnable {
      override def run(): Unit = {
        for(isComplete <- Task.this.atomic.isComplete()) {
          if(isComplete) {
            Task.this.atomic.result().onComplete({
              case Success(x) =>
                promise.success(x)
              case Failure(x) =>
                promise.failure(x)
            })
          }
        }
      }
    }, 1, TimeUnit.SECONDS)
    promise.future.onComplete({case _ => schedule.cancel(false)})
    promise.future
  }

  def atomicObtainTask(executorId: String = UUID.randomUUID().toString)(implicit cluster: Restm, executionContext: ExecutionContext) = {
    new STMTxn[Option[(Restm, ExecutionContext) => TaskResult[T]]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        obtainTask(executorId)
      }
    }.txnRun(cluster)(executionContext)
  }

  def obtainTask(executorId: String = UUID.randomUUID().toString)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.read().map(currentState => {
      val task: Option[(Restm, ExecutionContext) => TaskResult[T]] = currentState.kryoTask.flatMap(x=>x.deserialize())
      task.filter(_ => {
        currentState.executorId.isEmpty && currentState.result.isEmpty && currentState.exception.isEmpty
      }).map(task => {
        root.write(currentState.copy(executorId = Option(executorId)))
        task
      })
    })
  }

  def addSubscriber(task: Task[_], queue: StmExecutionQueue)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
    root.read.flatMap(prev=>{
      val newSubscribers: Map[Task[_], StmExecutionQueue] = Map[Task[_], StmExecutionQueue](task -> queue)
      val prevSubscribers: List[TaskSubscription] = prev.subscribers
      root.write(prev.copy(subscribers = prevSubscribers ++ newSubscribers.map(e=>new TaskSubscription(e._1, e._2)).toList)).map(_ => prev)
    }).map(currentState=>{
      (currentState.result.isDefined || currentState.exception.isDefined)
    })
  }

  def initTriggers(queue: StmExecutionQueue)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.read.flatMap(_.initTriggers(Task.this, queue)).map(_=>Unit)
  }

  def result()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[T] = {
    root.read().map(currentState => currentState.exception.map(throw _).orElse(currentState.result).get)
  }

  def canRun()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.read().flatMap(currentState => {
      Future.sequence(currentState.triggers.map(_.isComplete())).map(_.reduceOption(_&&_).getOrElse(true))
    })
  }

  def atomicCanRun()(implicit cluster: Restm, executionContext: ExecutionContext) = {
    new STMTxn[Boolean] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        canRun()(ctx,executionContext)
      }
    }.txnRun(cluster)(executionContext)
  }

  def complete(result:Try[TaskResult[T]])(implicit cluster: Restm, executionContext: ExecutionContext): Future[Unit] = {
    new STMTxn[List[TaskSubscription]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        root.read().flatMap(currentState => {
          result.transform[Future[TaskData[T]]](
            taskResult => Try {
              taskResult match {
                case TaskSuccess(result) =>
                  Future.successful(currentState.copy(result = Option(result)))
                case TaskContinue(queue, next, newTriggers) =>
                  currentState
                    .copy(triggers = newTriggers, executorId = None)
                    .newTask(next)
                    .initTriggers(Task.this, queue)
              }
            },
            exception => Try {
              Future.successful(currentState.copy(exception = Option(exception)))
            }
          ).get.flatMap(update=>root.write(update)).map(_=>currentState.subscribers)
        })
      }
    }.txnRun(cluster)(executionContext).flatMap(subscribers => {
      Future.sequence(subscribers.map(subscription => {
        subscription.task.atomicCanRun().flatMap(complete => {
          if (complete) {
            subscription.queue.atomic.add(subscription.task)
          } else {
            Future.successful(Unit)
          }
        })
      })).map(_ => Unit)
    })
  }

  def run(cluster: Restm, executionContext: ExecutionContext) : Future[Unit] = {
    implicit val _cluster = cluster
    implicit val _executionContext = executionContext
    atomicObtainTask().flatMap(_
      .map(task => Try { task(cluster, executionContext) })
      .map(result => complete( result ))
      .getOrElse(Future.successful(Unit)))
  }
}

case class TaskSubscription(task: Task[_], queue:StmExecutionQueue)

case class TaskData[T](
                              kryoTask : Option[KryoValue] = None,
                              executorId : Option[String] = None,
                              result : Option[T] = None,
                              exception: Option[Throwable] = None,
                              triggers: List[Task[_]] = List.empty,
                              subscribers: List[TaskSubscription] = List.empty
                              ) {

  def newTask(task : (Restm, ExecutionContext) => TaskResult[T]) = {
    this.copy(kryoTask = Option(KryoValue(task)))
  }

  def initTriggers(t:Task[T], queue: StmExecutionQueue)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TaskData[T]] = {
    Future.sequence(this.triggers.map(_.addSubscriber(t, queue))).map(_.reduceOption(_ && _).getOrElse(true)).flatMap(allTriggersTripped => {
      if (allTriggersTripped) {
        queue.add(t).map(_=>TaskData.this)
      } else {
        Future.successful(TaskData.this)
      }
    })
  }
}
