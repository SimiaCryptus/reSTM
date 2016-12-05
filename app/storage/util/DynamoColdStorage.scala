package storage.util

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model._
import storage.ColdStorage
import storage.Restm._

import scala.collection.JavaConverters._


class DynamoColdStorage(tableName:String,
                        awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain,
                        endpoint: String = "https://dynamodb.us-east-1.amazonaws.com"
                       ) extends ColdStorage {
  private val client: AmazonDynamoDBClient = {
    val client: AmazonDynamoDBClient = new AmazonDynamoDBClient(awsCredentialsProvider)
    client.setEndpoint(endpoint)
    client
  }
  private lazy val dynamoDB = new DynamoDB(client)
  private lazy val table = {
    val tableExists = {
      val raw = Option(dynamoDB.listTables())
      val tables: List[Table] = raw.map(_.iterator().asScala.toList).getOrElse(List.empty)
      tables.exists(_.getTableName == tableName)
    }
    if(!tableExists) dynamoDB.createTable(new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(
        new KeySchemaElement("ptrId", KeyType.HASH),
        new KeySchemaElement("txnTime", KeyType.RANGE)
      ).withAttributeDefinitions(
        new AttributeDefinition().withAttributeName("ptrId").withAttributeType(ScalarAttributeType.S),
        new AttributeDefinition().withAttributeName("txnTime").withAttributeType(ScalarAttributeType.S)
      ).withProvisionedThroughput(new ProvisionedThroughput(10l,10l))
    )
    dynamoDB.getTable(tableName)
  }
  def store(id: PointerType, data : Map[TimeStamp, ValueType]) = {
    for(d <- data) {
      table.putItem(new Item()
        .withPrimaryKey(new PrimaryKey().addComponent("ptrId",id.toString).addComponent("txnTime",d._1.toString))
        .withString("value", d._2.toString))
    }
  }
  def read(id: PointerType) : Map[TimeStamp, ValueType] = {
    table.query(new KeyAttribute("ptrId", id.toString)).iterator().asScala.map(item=>{
      new TimeStamp(item.getString("txnTime")) -> new ValueType(item.getString("value"))
    }).toMap
  }
}
