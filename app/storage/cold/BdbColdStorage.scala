package storage.cold

import java.io.File

import com.sleepycat.je._
import storage.Restm._
import storage.types.KryoValue

class BdbColdStorage(path:String = "db", dbname:String = "db") extends ColdStorage {

  lazy val envConfig = {
    val envConfig = new EnvironmentConfig()
    envConfig.setAllowCreate(true)
    envConfig
  }
  lazy val env = {
    val envHome = new File(path)
    if(!envHome.exists()) envHome.mkdir()
    val env = new Environment(envHome, envConfig)
    env
  }

  lazy val db = env.openDatabase(null, "stmdata", dbConfig)
  lazy val dbConfig = {
    val dbConfig = new DatabaseConfig()
    dbConfig.setAllowCreate(true)
    dbConfig
  }

  def store(id: PointerType, data : Map[TimeStamp, ValueType]) = {
    val keyEntry = new DatabaseEntry(id.toString.getBytes("UTF-8"))
    val valueEntry = new DatabaseEntry(KryoValue(data ++ read(id)).toString.getBytes("UTF-8"))
    db.put(null, keyEntry, valueEntry)
  }

  def read(id: PointerType) : Map[TimeStamp, ValueType] = {
    val keyEntry = new DatabaseEntry(id.toString.getBytes("UTF-8"))
    val valueEntry = new DatabaseEntry()
    if (db.get(null, keyEntry, valueEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
      new KryoValue(new String(valueEntry.getData(), "UTF-8")).deserialize[Map[TimeStamp, ValueType]]().get
    } else {
      Map.empty
    }
  }

  override def clear() = throw new RuntimeException("Not implemented")

  def close() = {
    db.close()
    env.close()
  }

}
