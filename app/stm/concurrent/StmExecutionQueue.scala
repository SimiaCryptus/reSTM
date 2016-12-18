package stm.concurrent

import java.util.Date

import _root_.util.Util._
import stm.collection.LinkedList
import stm.concurrent.Task._
import stm.{AtomicApiBase, STMTxn, STMTxnCtx, SyncApiBase}
import storage.Restm
import storage.Restm.PointerType

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try}


object StmExecutionQueue extends StmExecutionQueue(LinkedList.static[Task[_]](new PointerType("StmExecutionQueue/workQueue"))) {
}

class StmExecutionQueue(val workQueue: LinkedList[Task[_]]) {

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
    workQueue.add(f, 0.2)
  }

  var verbose = false

  def task()(cluster: Restm, executionContext: ExecutionContext): Unit = {
    try {
      while (!Thread.interrupted()) {
        try {
          val future = runTask(cluster, executionContext)
          val result = Await.result(future, 10.minutes)
          if(!result) Thread.sleep(500)
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
    val executorName: String = ExecutionStatusManager.getName()
    val taskFuture = monitorFuture("StmExecutionQueue.getTask") {
      new STMTxn[Option[(Task[_], (Restm, ExecutionContext) => TaskResult[_])]] {
        override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
          workQueue.remove(0.2).map(_.map(_.asInstanceOf[Task[AnyRef]])).flatMap(task => {
            task.map(task => {
              task.obtainTask(executorName).map(_.map(function => {
                ExecutionStatusManager.start(executorName, task)
                task -> function
              }))
            }).getOrElse(Future.successful(None))
          })
        }
      }.txnRun(cluster)(executionContext)
    }
    taskFuture.flatMap(taskTuple=>{
      taskTuple.map(tuple => {
        val (task: Task[AnyRef], function) = tuple
        if (verbose) println(s"Starting task ${task.id} at ${new Date()}")
        val result = monitorBlock("StmExecutionQueue.runTask") { Try { function(cluster, executionContext) } }
          .recoverWith({ case e =>
            if (verbose) e.printStackTrace();
            Failure(e)
          }).asInstanceOf[Try[TaskResult[AnyRef]]]
        monitorFuture("StmExecutionQueue.completeTask") {
          task.atomic()(cluster, executionContext).complete(result)
        }.map(_ => {
          if (verbose) println(s"Completed task ${task.id} at ${new Date()}")
          ExecutionStatusManager.end(executorName, task)
          true
        })
      }).getOrElse(Future.successful(false))
    })(executionContext)

  }

  def registerDaemons(count: Int = 1)(implicit cluster: Restm, executionContext: ExecutionContext) = {
    (1 to count).map(i=>{
      val f: (Restm, ExecutionContext) => Unit = task() _
      DaemonConfig(s"Queue-${workQueue.id}-$i", f)
    }).foreach(daemon=>StmDaemons.config.atomic().sync.add(daemon))
  }

}
