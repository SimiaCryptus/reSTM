package stm.lib0

import java.util.Date

import stm.STMTxnCtx
import stm.lib0.Task.TaskResult
import storage.Restm
import storage.Restm.PointerType

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}


object StmExecutionQueue extends StmExecutionQueue {

  val workQueue = LinkedList.static[Task[_]](new PointerType("StmExecutionQueue/workQueue"))
  private val threads = new ArrayBuffer[Thread]
  var verbose = false

  private def task(implicit cluster: Restm, executionContext: ExecutionContext) = new Runnable {


    override def run(): Unit = {
      while (!Thread.interrupted()) {
        try {
          val item = workQueue.atomic.sync.remove(0.2)
          item.map(item=>{
            require(item.root != null)
            item.run(cluster, executionContext)
            if(verbose) println(s"Ran a task at ${new Date()}")
          }) getOrElse {
            Thread.sleep(100)
          }
        } catch {
          case e : Throwable =>
            e.printStackTrace()
            Thread.sleep(100)
        }
      }
    }
  }

  def start(count: Int = 1)(implicit cluster: Restm, executionContext: ExecutionContext) = {
    threads ++= (for (i <- 1 to count) yield {
      val thread: Thread = new Thread(task)
      thread.setDaemon(true)
      thread.setName(s"StmExecutionQueue-$i")
      thread.start()
      thread
    })
  }
}

trait StmExecutionQueue {
  def workQueue : LinkedList[Task[_]]

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
}
