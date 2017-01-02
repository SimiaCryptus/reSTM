package stm.task

import java.util.Date

import _root_.util.Util._
import stm.collection.IdQueue
import stm.task.Task._
import stm.{AtomicApiBase, STMTxn, STMTxnCtx, SyncApiBase}
import storage.Restm

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try}


object StmExecutionQueue {
  private var default : StmExecutionQueue = _

  def init()(implicit cluster: Restm, executionContext: ExecutionContext): Future[StmExecutionQueue] = {
    new STMTxn[StmExecutionQueue] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[StmExecutionQueue] = {
        IdQueue.create[Task[_]](5).map(new StmExecutionQueue(_))
      }
    }.txnRun(cluster).map(x=>{default = x;x})
  }
  def get(): StmExecutionQueue = {
    default
  }
  def reset()(implicit cluster: Restm, executionContext: ExecutionContext): Unit = {
    default = null
  }
}

class StmExecutionQueue(val workQueue: IdQueue[Task[_]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase{
    def add(f: Task[_]): Future[Unit] = atomic { StmExecutionQueue.this.add(f)(_,executionContext) }
    def add[T](f: (Restm, ExecutionContext)=>TaskResult[T], ancestors: List[Task[_]] = List.empty): Future[Task[T]] = atomic { StmExecutionQueue.this.add[T](f, ancestors)(_,executionContext) }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(f: Task[_]): Unit = sync { AtomicApi.this.add(f) }
      def add[T](f: (Restm, ExecutionContext)=>TaskResult[T], ancestors: List[Task[_]] = List.empty): Task[T] = sync { AtomicApi.this.add[T](f, ancestors) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add[T](f: (Restm, ExecutionContext)=>TaskResult[T], ancestors: List[Task[_]] = List.empty)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Task[T] =
      sync { StmExecutionQueue.this.add[T](f, ancestors) }
    def add(f: Task[_])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Unit = sync { StmExecutionQueue.this.add(f) }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)


  def add[T](f: (Restm, ExecutionContext)=>TaskResult[T], ancestors: List[Task[_]] = List.empty)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Task[T]] = {
    Task.create(f, ancestors).flatMap(task=>task.initTriggers(StmExecutionQueue.this).map(_=>task))
  }

  def add(f: Task[_])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Unit] = {
    workQueue.add(f)
  }

  var verbose = false

  def task()(cluster: Restm, executionContext: ExecutionContext): Unit = {
    try {
      while (!Thread.interrupted()) {
        try {
          val future = runTask(cluster, executionContext)
          val result = Await.result(future, 10.minutes)
          if(!result) Thread.sleep(1000)
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
          case e: Throwable =>
            e.printStackTrace()
        }
      }
    } catch {
      case e: Throwable =>
        new RuntimeException(s"Error in task executor",e).printStackTrace(System.err)
    }
  }

  private[this] def runTask(implicit cluster: Restm, executionContext: ExecutionContext): Future[Boolean] = {
    val executorId = ExecutionStatusManager.getName
    val taskFuture = monitorFuture("StmExecutionQueue.getTask") {
      new STMTxn[Option[(Task[_], (Restm, ExecutionContext) => TaskResult[_])]] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[(Task[AnyRef], (Restm, ExecutionContext) => TaskResult[AnyRef])]] = {
          getTask(executorId)
        }
      }.txnRun(cluster)(executionContext)
    }
    taskFuture.flatMap(taskTuple=>{
      taskTuple.map(tuple => {
        val function = tuple._2
        val task = tuple._1.asInstanceOf[Task[AnyRef]]
        if (verbose) println(s"Starting task ${task.id} at ${new Date()}")
        val result = monitorBlock("StmExecutionQueue.runTask") {
          def attempt(retries:Int): TaskResult[_] = {
            try {
              function(cluster, executionContext)
            } catch {
              case _: Throwable if retries > 0 => attempt(retries-1)
            }
          }
          Try { attempt(3) }
        }.recoverWith({ case e =>
            e.printStackTrace()
            Failure(e)
          }).asInstanceOf[Try[TaskResult[AnyRef]]]
        monitorFuture("StmExecutionQueue.completeTask") {
          task.atomic()(cluster, executionContext).complete(result)
        }.map(_ => {
          if (verbose) println(s"Completed task ${task.id} at ${new Date()}")
          ExecutionStatusManager.end(executorId, task)
          true
        })
      }).getOrElse(Future.successful(false))
    })(executionContext)

  }

  private def getTask(executorName: String = ExecutionStatusManager.getName)
                     (implicit ctx: STMTxnCtx, executionContext: ExecutionContext) : Future[Option[(Task[AnyRef], (Restm, ExecutionContext) => TaskResult[AnyRef])]] = {
    workQueue.take(2).map(_.map(_.asInstanceOf[Task[AnyRef]])).flatMap((task: Option[Task[AnyRef]]) => {
      task.map(task => {
        val obtainTask: Future[Option[(Restm, ExecutionContext) => TaskResult[AnyRef]]] = task.obtainTask(executorName)
        obtainTask.map(_.map(function => {
          ExecutionStatusManager.start(executorName, task)
          task -> function
        }))
      }).getOrElse(Future.successful(None))
    })
  }

  def registerDaemons(count: Int = 1)(implicit cluster: Restm, executionContext: ExecutionContext): Unit = {
    (1 to count).map(i=>{
      val f: (Restm, ExecutionContext) => Unit = task()
      DaemonConfig(s"Queue-${workQueue.id}-$i", f)
    }).foreach(daemon=>StmDaemons.config.atomic().sync.add(daemon))
  }

}
