package stm.collection

import java.lang.Double

import stm._
import storage.Restm.PointerType
import storage.{Restm, TransactionConflict}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

object DistributedScalar {
  case class ScalarData
  (
    values : List[STMPtr[java.lang.Double]] = List.empty
  ) {

    def add(value: Double, rootPtr: STMPtr[ScalarData])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
      val shuffledLists = values.map(_->Random.nextDouble()).sortBy(_._2).map(_._1)
      def add(list : Seq[STMPtr[java.lang.Double]] = shuffledLists): Future[Unit] = {
        if(list.isEmpty) throw new TransactionConflict("Could not lock any queue") else {
          val head = list.head
          val tail: Seq[STMPtr[java.lang.Double]] = list.tail
          head.lock().flatMap(locked=>{
            if(locked) {
              head.read().map(_+value).flatMap(head.write(_))
            } else {
              add(tail)
            }
          })
        }
      }
      add()
    }

    def get(rootPtr: STMPtr[ScalarData])(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Double] = {
      Future.sequence(values.map(_.read()))
        .map((sequence: List[java.lang.Double]) => {
          sequence.map(_.toDouble).reduceOption(_ + _).getOrElse(0.0).toDouble
        })
    }

  }
  def create(size : Int = 8)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[DistributedScalar] =
    Future.sequence((1 to size).map(_=>STMPtr.dynamic(Double.valueOf(0.0)))).flatMap(ptrs=>{
      STMPtr.dynamic(new DistributedScalar.ScalarData(ptrs.toList)).map(new DistributedScalar(_))
    })

  def createSync(size : Int = 8)(implicit cluster: Restm, executionContext: ExecutionContext) =
    Await.result(new STMTxn[DistributedScalar] {
      override def txnLogic()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[DistributedScalar] = {
        create(size)
      }
    }.txnRun(cluster),60.seconds)

}

class DistributedScalar(rootPtr: STMPtr[DistributedScalar.ScalarData]) {
  def id = rootPtr.id.toString

  def this(ptr:PointerType) = this(new STMPtr[DistributedScalar.ScalarData](ptr))

  class AtomicApi(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) extends AtomicApiBase(priority,maxRetries) {
    class SyncApi(duration: Duration) extends SyncApiBase(duration) {
      def add(value:Double) = sync { AtomicApi.this.add(value) }
      def get() = sync { AtomicApi.this.get() }
    }
    def sync(duration: Duration) = new SyncApi(duration)
    def sync = new SyncApi(10.seconds)
    def add(value:Double) = atomic { DistributedScalar.this.add(value)(_,executionContext).map(_ => Unit) }
    def get() = atomic { DistributedScalar.this.get()(_,executionContext) }
  }
  def atomic(priority: Duration = 0.seconds, maxRetries:Int = 1000)(implicit cluster: Restm, executionContext: ExecutionContext) = new AtomicApi(priority,maxRetries)
  class SyncApi(duration: Duration) extends SyncApiBase(duration) {
    def add(value:Double)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { DistributedScalar.this.add(value) }
    def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = sync { DistributedScalar.this.get() }
  }
  def sync(duration: Duration) = new SyncApi(duration)
  def sync = new SyncApi(10.seconds)

  def add(value:Double)(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    getInner().flatMap(inner => {
      inner.add(value, rootPtr)
    })
  }

  def get()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext): Future[Double] = {
    getInner().flatMap(inner => {
      inner.get(rootPtr)
    })
  }

  private def getInner()(implicit ctx: STMTxnCtx, executionContext: ExecutionContext) = {
    rootPtr.readOpt().map(_.orElse(Option(new DistributedScalar.ScalarData()))).map(_.get)
  }
}

