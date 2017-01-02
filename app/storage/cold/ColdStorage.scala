package storage.cold

import storage.Restm._

trait ColdStorage {
  def clear(): Unit = {}

  def store(id: PointerType, data: Map[TimeStamp, ValueType]): Unit

  def read(id: PointerType): Map[TimeStamp, ValueType]
}




