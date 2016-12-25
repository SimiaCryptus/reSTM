package storage.actors

import java.util.concurrent.atomic.AtomicBoolean

import storage.TransactionConflict
import util.Util

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

trait ActorQueue {
  private[this] val queue = new java.util.concurrent.ConcurrentLinkedQueue[() => Unit]()
  private[this] val guard = new AtomicBoolean(false)
  protected def messageNumber() = processedMessages
  private[this] var processedMessages = 0

  private[this] var closed = false
  def close() = closed = true

  def withActor[T](f: => T)(implicit exeCtx: ExecutionContext): Future[T] = Util.chainEx("Error running actor task") {
    if(closed) throw new TransactionConflict(s"Actor ${ActorQueue.this} is closed")
    val promise = Promise[T]()
    queue.add(() => {
      val result: Try[T] = Try {
        if(closed) throw new TransactionConflict(s"Actor ${ActorQueue.this} is closed")
        val result = f
        processedMessages += 1
        result
      }.recover(errorHandler)
      promise.complete(result)
    })
    queueBatch
    promise.future
  }

  def errorHandler[T](implicit exeCtx: ExecutionContext): PartialFunction[Throwable, T] = {
    case e: Throwable =>
      log(s"$this Error - " + e.getMessage)
      throw e
  }

  def withFuture[T](f: => Future[T])(implicit exeCtx: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    queue.add(() => {
      promise.completeWith(f.recover(errorHandler))
    })
    queueBatch
    promise.future
  }

  private[this] def queueBatch[T](implicit exeCtx: ExecutionContext): Future[Unit] = {
    Future {
      if (!queue.isEmpty && !guard.getAndSet(true)) {
        try {
          for (task <- Stream.continually(queue.poll()).takeWhile(null != _).take(100)) {
            task()
          }
        } finally {
          guard.set(false)
        }
        if (!queue.isEmpty) queueBatch
      }
    }
  }

  def log(str: String)(implicit exeCtx: ExecutionContext): Future[Unit] = ActorLog.log(str)

}

