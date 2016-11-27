package storage.util

import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.inject.Singleton

import storage.Restm.{PointerType, TimeStamp, ValueType}
import storage._

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class RestmInject extends RestmPtr with RestmInternalPtr {
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  override lazy val inner = new RestmImpl(new RestmActors)
  override lazy val _inner: RestmInternal = inner.internal

  override def queueValue(id: PointerType, time: TimeStamp, value: ValueType): Future[Unit] = inner.queueValue(id, time, value)

}
