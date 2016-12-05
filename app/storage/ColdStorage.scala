package storage

import storage.Restm._

import scala.collection.concurrent.TrieMap

trait ColdStorage {
  def store(id: PointerType, data: Map[TimeStamp, ValueType]) : Unit
  def read(id: PointerType) : Map[TimeStamp, ValueType]
}

class HeapColdStorage extends ColdStorage {
  val mem = new TrieMap[PointerType, TrieMap[TimeStamp, ValueType]]()
  def store(id: PointerType, data : Map[TimeStamp, ValueType]) = {
    mem.getOrElseUpdate(id, new TrieMap[TimeStamp, ValueType]) ++= data
  }
  def read(id: PointerType) : Map[TimeStamp, ValueType] = {
    mem.getOrElseUpdate(id, new TrieMap[TimeStamp, ValueType]).toMap
  }
}


