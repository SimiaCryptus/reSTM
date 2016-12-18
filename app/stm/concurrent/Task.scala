package stm.concurrent

import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.fasterxml.jackson.annotation.JsonIgnore
import stm._
import stm.concurrent.Task.TaskResult
import stm.concurrent.TaskStatus.Failed
import storage.Restm
import storage.Restm.PointerType
import storage.data.KryoValue

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object Task {
  def create[T](f: (Restm, ExecutionContext) => TaskResult[T], ancestors: List[Task[_]] = List.empty)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    STMPtr.dynamic[TaskData[T]](new TaskData[T](Option(KryoValue(f)), triggers = ancestors)).map(new Task(_))

  def apply[T](id: PointerType) = new Task(STMPtr.static[TaskData[T]](id, new TaskData[T]()))

  sealed trait TaskResult[T] {
  }
  final case class TaskSuccess[T](value:T) extends TaskResult[T]
  final case class TaskContinue[T](queue: StmExecutionQueue, newFunction: (Restm, ExecutionContext) => TaskResult[T], newTriggers:List[Task[T]] = List.empty) extends TaskResult[T]

  private[stm] val scheduledThreadPool: ScheduledExecutorService = Executors.newScheduledThreadPool(5)
  private[Task] val futureCache = new TrieMap[AnyRef, Future[_]]()
}
import stm.concurrent.Task._

object TaskStatus {
  object Blocked extends TaskStatus
  object Queued extends TaskStatus
  case class Running(executor : String) extends TaskStatus
    { override def toString: String = s"${getClass.getSimpleName}($executor)" }
  object Success extends TaskStatus
  case class Failed(ex : String) extends TaskStatus
  { override def toString: String = s"${getClass.getSimpleName}($ex)" }
  object Unknown extends TaskStatus
  case class Orphan(ex : String) extends TaskStatus
  { override def toString: String = s"${getClass.getSimpleName}($ex)" }
  object Ex_NotDefined extends TaskStatus
}
sealed class TaskStatus {
  def getName() = getClass.getSimpleName
  override def toString: String = s"${getClass.getSimpleName}"
}

case class TaskStatusTrace(id:PointerType, status: TaskStatus, children: List[TaskStatusTrace] = List.empty)

class Task[T](val root : STMPtr[TaskData[T]]) {


  class AtomicApi(priority: Duration = 0.seconds)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority) {
    def result() = atomic { Task.this.result()(_,executionContext) }
    def isComplete() = atomic { Task.this.isComplete()(_,executionContext) }
    def complete(result:Try[TaskResult[T]]) = atomic { Task.this.complete(result)(_,executionContext) }
    def getStatusTrace(queue: Option[StmExecutionQueue] = None) = atomic { Task.this.getStatusTrace(queue)(_,executionContext) }
    def obtainTask(executorId: String = UUID.randomUUID().toString) = atomic { Task.this.obtainTask(executorId)(_,executionContext) }
    def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U]) = atomic { Task.this.map(queue, function)(_,executionContext) }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def result() = sync { AtomicApi.this.result() }
      def isComplete() = sync { AtomicApi.this.isComplete() }
      def complete(result:Try[TaskResult[T]]) = sync { AtomicApi.this.complete(result) }
      def getStatusTrace(queue: Option[StmExecutionQueue] = None) = sync { AtomicApi.this.getStatusTrace(queue) }
      def obtainTask(executorId: String = UUID.randomUUID().toString) = sync { AtomicApi.this.obtainTask(executorId) }
      def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U]) = sync { AtomicApi.this.map(queue, function) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(priority: Duration = 0.seconds)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority)
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def result()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.result() }
    def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.map(queue, function) }
    def isComplete()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.isComplete() }
    def complete(result:Try[TaskResult[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.complete(result) }
    def getStatusTrace(queue: Option[StmExecutionQueue] = None)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.getStatusTrace(queue) }
    def obtainTask(executorId: String = UUID.randomUUID().toString)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { Task.this.obtainTask(executorId) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)

  def id = root.id

  def map[U](queue : StmExecutionQueue, function: (T, Restm, ExecutionContext) => TaskResult[U])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Task[U]] = {
    val func: (Restm, ExecutionContext) => TaskResult[U] = wrapMap(function)
    Task.create(func, List(Task.this)).flatMap(task=>task.initTriggers(queue).map(_=>task))
  }

  def wrapMap[U](function: (T, Restm, ExecutionContext) => U): (Restm, ExecutionContext) => U = {
    (c, e) => function(Task.this.atomic()(c, e).sync.result(), c, e)
  }

  def isComplete()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.readOpt().map(_.map(currentState => currentState.result.isDefined || currentState.exception.isDefined).getOrElse(false))
  }

  def future(implicit cluster: Restm, executionContext: ExecutionContext) = futureCache.getOrElseUpdate((cluster,this,executionContext),{
    val promise = Promise[T]()
    val schedule: ScheduledFuture[_] = scheduledThreadPool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Task.this.atomic().isComplete().map(isComplete=> {
        //System.out.println(s"Checked ${Task.this.id} - isComplete = $isComplete")
        if(isComplete) { promise.completeWith(Task.this.atomic().result()) }
      })}, 1, 1, TimeUnit.SECONDS)
    promise.future.onComplete({case _ => schedule.cancel(false)})
    promise.future
  }).asInstanceOf[Future[T]]

  def obtainTask(executorId: String = UUID.randomUUID().toString)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Option[(Restm, ExecutionContext) => TaskResult[T]]] = {
    require(null != root)
    root.read().flatMap(currentState => {
      val task: Option[(Restm, ExecutionContext) => TaskResult[T]] = currentState.kryoTask.flatMap(x=>x.deserialize())
      task.filter(_ => {
        currentState.executorId.isEmpty && currentState.result.isEmpty && currentState.exception.isEmpty
      }).map(task => {
        root.write(currentState.copy(executorId = Option(executorId))).map(_=>task).map(Option(_))
      }).getOrElse(Future.successful(None))
    })
  }

  def addSubscriber(task: Task[_], queue: StmExecutionQueue)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Boolean] = {
    root.read.flatMap(prev=>{
      val newSubscribers: Map[Task[_], StmExecutionQueue] = Map[Task[_], StmExecutionQueue](task -> queue)
      val prevSubscribers: List[TaskSubscription] = prev.subscribers
      root.write(prev.copy(subscribers = prevSubscribers ++ newSubscribers.map(e=>new TaskSubscription(e._1, e._2)).toList)).map(_ => prev)
    }).map(currentState=>currentState.result.isDefined || currentState.exception.isDefined)
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

  @JsonIgnore def getStatusTrace(queue: Option[StmExecutionQueue])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[TaskStatusTrace] = {
    val queuedTasks = queue.map(_.workQueue.sync.stream().map(_.id).toSet).getOrElse(Set.empty)
    //System.out.println(s"Current tasks: ${queuedTasks.size}")
    val threads: Iterable[Thread] = Thread.getAllStackTraces.asScala.keys
    getStatusTrace(queuedTasks, threads)
  }

  def getStatusTrace(queuedTasks: Set[PointerType], threads: Iterable[Thread])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[TaskStatusTrace] = {
    def visit(currentState: TaskData[T], id: PointerType): TaskStatusTrace = {
      val status: TaskStatus = if (currentState.result.isDefined) {
        TaskStatus.Success
      } else if (currentState.exception.isDefined) {
        new TaskStatus.Failed(currentState.exception.get.toString)
      } else if (currentState.executorId.isDefined) {
        if (checkExecutor(currentState.executorId.get, id, threads)) {
          new TaskStatus.Running(currentState.executorId.get)
        } else {
          new TaskStatus.Orphan("Not Found: " + currentState.executorId.get)
        }
      } else {
        TaskStatus.Unknown
      }
      new TaskStatusTrace(id, status, List.empty)
    }
    root.readOpt().flatMap(currentState => {
      val statusTrace: TaskStatusTrace = currentState.map(visit(_, root.id)).getOrElse(new TaskStatusTrace(root.id, TaskStatus.Ex_NotDefined))
      lazy val children: Future[List[TaskStatusTrace]] = currentState.map(_.triggers.map(_.getStatusTrace(queuedTasks, threads))).map(Future.sequence(_)).getOrElse(Future.successful(List.empty))
      statusTrace.status match {
        case TaskStatus.Success => Future.successful(statusTrace)
        case TaskStatus.Unknown => children.map(children => {
          val pending = children.filter(_.status match {
            case TaskStatus.Success => false
            case _: Failed => false
            case _ => true
          })
          if (pending.isEmpty) {
            if (queuedTasks.contains(id)) {
              statusTrace.copy(status = TaskStatus.Queued)
            } else {
              statusTrace.copy(status = new TaskStatus.Orphan("Not In Queue"))
            }
          }
          else statusTrace.copy(status = TaskStatus.Blocked, children = children)
        })
        case _: Failed => Future.successful(statusTrace)
        case TaskStatus.Ex_NotDefined => Future.successful(statusTrace)
        case _ => children.map(x => statusTrace.copy(children = x))
      }
    })
  }

  def checkExecutor(executorId: String, id: PointerType, threads: Iterable[Thread]): Boolean = {
    def isExecutorAlive = {
      threads.filter(_.isAlive)
        .exists(_.getName == executorId.split(":")(1))
    }
    def isCurrentTask = ExecutionStatusManager.check(executorId, id)
    isCurrentTask && isExecutorAlive
  }

  def complete(result:Try[TaskResult[T]])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Unit] = {
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
    }).flatMap(subscribers => {
      Future.sequence(subscribers.map(subscription => {
        subscription.task.canRun().flatMap(complete => {
          if (complete) {
            subscription.queue.add(subscription.task)
          } else {
            Future.successful(Unit)
          }
        })
      })).map(_ => Unit)
    })
  }

  override def equals(other: Any): Boolean = other match {
    case that: Task[T] => root == that.root
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(root)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
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
