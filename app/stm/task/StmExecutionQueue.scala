package stm.task

import java.util.Date

import _root_.util.Util._
import stm.collection.IdQueue
import stm.task.Task._
import stm.{AtomicApiBase, STMTxn, STMTxnCtx, SyncApiBase}
import storage.Restm

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try}


object StmExecutionQueue {
  private var default : StmExecutionQueue = null

  def init()(implicit cluster: Restm, executionContext: ExecutionContext) = {
    new STMTxn[StmExecutionQueue] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[StmExecutionQueue] = {
        IdQueue.create[Task[_]](5).map(new StmExecutionQueue(_))
      }
    }.txnRun(cluster).map(x=>{default = x;x})
  }
  def get() = {
    default
  }
  def reset()(implicit cluster: Restm, executionContext: ExecutionContext) = {
    default = null
  }
}

class StmExecutionQueue(val workQueue: IdQueue[Task[_]]) {

  class AtomicApi()(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase{
    def add(f: Task[_]) = atomic { StmExecutionQueue.this.add(f)(_,executionContext) }
    def add[T](f: (Restm, ExecutionContext)=>TaskResult[T], ancestors: List[Task[_]] = List.empty) = atomic { StmExecutionQueue.this.add[T](f, ancestors)(_,executionContext) }
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(f: Task[_]) = sync { AtomicApi.this.add(f) }
      def add[T](f: (Restm, ExecutionContext)=>TaskResult[T], ancestors: List[Task[_]] = List.empty) = sync { AtomicApi.this.add[T](f, ancestors) }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
  }
  def atomic(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add[T](f: (Restm, ExecutionContext)=>TaskResult[T], ancestors: List[Task[_]] = List.empty)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) =
      sync { StmExecutionQueue.this.add[T](f, ancestors) }
    def add(f: Task[_])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { StmExecutionQueue.this.add(f) }
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
          case e: InterruptedException =>
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
    val taskFuture = monitorFuture("StmExecutionQueue.getTask") {
      new STMTxn[Option[(Task[_], (Restm, ExecutionContext) => TaskResult[_])]] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          getTask()
        }
      }.txnRun(cluster)(executionContext)
    }
    taskFuture.flatMap(taskTuple=>{
      taskTuple.map(tuple => {
        val (task: Task[AnyRef], function) = tuple
        if (verbose) println(s"Starting task ${task.id} at ${new Date()}")
        val result = monitorBlock("StmExecutionQueue.runTask") {
          val tries: Seq[Try[TaskResult[_]]] = (1 to 3).map(_ => Try {
            function(cluster, executionContext)
          })
          tries.find(_.isSuccess).getOrElse(tries.head)
        }.recoverWith({ case e =>
            e.printStackTrace()
            Failure(e)
          }).asInstanceOf[Try[TaskResult[AnyRef]]]
        monitorFuture("StmExecutionQueue.completeTask") {
          task.atomic()(cluster, executionContext).complete(result)
        }.map(_ => {
          if (verbose) println(s"Completed task ${task.id} at ${new Date()}")
          ExecutionStatusManager.end(ExecutionStatusManager.getName(), task)
          true
        })
      }).getOrElse(Future.successful(false))
    })(executionContext)

  }

  private def getTask()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Option[(Task[AnyRef], (Restm, ExecutionContext) => TaskResult[AnyRef])]] = {
    val executorName: String = ExecutionStatusManager.getName()
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

  def registerDaemons(count: Int = 1)(implicit cluster: Restm, executionContext: ExecutionContext) = {
    (1 to count).map(i=>{
      val f: (Restm, ExecutionContext) => Unit = task() _
      DaemonConfig(s"Queue-${workQueue.id}-$i", f)
    }).foreach(daemon=>StmDaemons.config.atomic().sync.add(daemon))
  }

}
