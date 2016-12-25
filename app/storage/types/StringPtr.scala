package storage.types

import java.util.UUID

class StringPtr(val id: String) {

  def this() = this(UUID.randomUUID().toString)

  override def toString: String = id.toString

  override def hashCode(): Int = id.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: StringPtr => id == x.id
    case _ => false
  }

}
