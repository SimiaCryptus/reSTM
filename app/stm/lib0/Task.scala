package stm.lib0

import java.util.UUID

import stm.{STMPtr, STMTxn, STMTxnCtx}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


object Task {
  def create[T](f: (Restm, ExecutionContext) => T, ancestors: List[Task[_]] = List.empty)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
    STMPtr.dynamic[TaskData[T]](new TaskData[T](task = Option(f), triggers = ancestors)).map(new Task(_))

  def static[T](id: PointerType) = new Task(STMPtr.static[TaskData[T]](id, new TaskData[T]))

}

class Task[T](root : STMPtr[TaskData[T]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase{
    //def add(value: T) = atomic { LinkedList.this.add(value) }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      //def add(value: T) = sync { AtomicApi.this.add(value) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    //def add(value: T) = sync { LinkedList.this.add(value) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def isComplete()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.read().map(currentState => currentState.result.isDefined || currentState.exception.isDefined)
  }

  def atomicObtainTask(executorId: String = UUID.randomUUID().toString)(implicit cluster: Restm, executionContext: ExecutionContext) = {
    new STMTxn[Option[(Restm, ExecutionContext) => T]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        obtainTask(executorId)
      }
    }.txnRun(cluster)(executionContext)
  }

  def obtainTask(executorId: String = UUID.randomUUID().toString)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.read().map(currentState => {
      currentState.task.filter(_ => {
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
      val prevSubscribers: Map[Task[_], StmExecutionQueue] = prev.subscribers
      root.write(prev.copy(subscribers = prevSubscribers ++ newSubscribers)).map(_ => prev)
    }).map(currentState=>{
      (currentState.result.isDefined || currentState.exception.isDefined)
    })
  }

  def initTriggers(queue: StmExecutionQueue)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    root.read.flatMap(prev=>{
      Future.sequence(prev.triggers.map(_.addSubscriber(Task.this, queue))).map(_.reduceOption(_&&_).getOrElse(true)).flatMap(allTriggersTripped=>{
        if(allTriggersTripped) {
          queue.add(Task.this)
        } else {
          Future.successful(Unit)
        }
      })
    })
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

  def complete(result:Try[T])(implicit cluster: Restm, executionContext: ExecutionContext): Future[Unit] = {
    new STMTxn[Map[Task[_], StmExecutionQueue]] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
        root.read().map(currentState => {
          root.write(result.transform[TaskData[T]](
            result => Try {
              currentState.copy(result = Option(result))
            },
            exception => Try {
              currentState.copy(exception = Option(exception))
            }
          ).get)
          currentState.subscribers
        })
      }
    }.txnRun(cluster)(executionContext).flatMap(subscribers => {
      Future.sequence(subscribers.map(subscriberTuple => {
        val task : Task[_] = subscriberTuple._1
        task.atomicCanRun().flatMap(complete => {
          if (complete) {
            subscriberTuple._2.atomic.add(task)
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


private case class TaskData[T](
                              task : Option[(Restm, ExecutionContext) => T] = None,
                              executorId : Option[String] = None,
                              result : Option[T] = None,
                              exception: Option[Throwable] = None,
                              triggers: List[Task[_]] = List.empty,
                              subscribers: Map[Task[_], StmExecutionQueue] = Map.empty
                              )