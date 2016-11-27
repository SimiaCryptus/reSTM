package storage.data

import java.util.UUID

class UUIDPtr(val id : UUID) {
  def this(str:String) = this({
    UUID.fromString(str)
  })

  def this() = this(UUID.randomUUID())

  override def toString: String = id.toString

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x : UUIDPtr => id == x.id
    case _ => false
  }

}
