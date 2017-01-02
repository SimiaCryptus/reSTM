/*
 * Copyright (c) 2017 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package storage.cold

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model._
import storage.Restm._

import scala.collection.JavaConverters._


class DynamoColdStorage(tableName: String,
                        awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain,
                        endpoint: String = "https://dynamodb.us-east-1.amazonaws.com"
                       ) extends ColdStorage {
  private lazy val dynamoDB = new DynamoDB(client)
  private lazy val table = {
    val tableExists = {
      val raw = Option(dynamoDB.listTables())
      val tables: List[Table] = raw.map(_.iterator().asScala.toList).getOrElse(List.empty)
      tables.exists(_.getTableName == tableName)
    }
    if (!tableExists) dynamoDB.createTable(new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(
        new KeySchemaElement("ptrId", KeyType.HASH),
        new KeySchemaElement("txnTime", KeyType.RANGE)
      ).withAttributeDefinitions(
      new AttributeDefinition().withAttributeName("ptrId").withAttributeType(ScalarAttributeType.S),
      new AttributeDefinition().withAttributeName("txnTime").withAttributeType(ScalarAttributeType.S)
    ).withProvisionedThroughput(new ProvisionedThroughput(10l, 10l))
    )
    dynamoDB.getTable(tableName)
  }
  private val client: AmazonDynamoDBClient = {
    val client: AmazonDynamoDBClient = new AmazonDynamoDBClient(awsCredentialsProvider)
    client.setEndpoint(endpoint)
    client
  }

  def store(id: PointerType, data: Map[TimeStamp, ValueType]): Unit = {
    for (d <- data) {
      table.putItem(new Item()
        .withPrimaryKey(new PrimaryKey().addComponent("ptrId", id.toString).addComponent("txnTime", d._1.toString))
        .withString("value", d._2.toString))
    }
  }

  def read(id: PointerType): Map[TimeStamp, ValueType] = {
    table.query(new KeyAttribute("ptrId", id.toString)).iterator().asScala.map(item => {
      new TimeStamp(item.getString("txnTime")) -> new ValueType(item.getString("value"))
    }).toMap
  }
}
