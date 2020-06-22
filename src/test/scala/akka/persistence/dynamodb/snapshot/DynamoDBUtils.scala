/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.snapshot

import java.util.UUID

import akka.actor.ActorSystem
import akka.util.Timeout
import software.amazon.awssdk.services.dynamodb.model._
import scala.collection.JavaConverters._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import akka.persistence.dynamodb.dynamoClient

trait DynamoDBUtils {

  def system: ActorSystem
  implicit val executionContext = system.dispatcher

  lazy val settings: DynamoDBSnapshotConfig = {
    val c = system.settings.config
    val config = c.getConfig(c.getString("akka.persistence.snapshot-store.plugin"))
    new DynamoDBSnapshotConfig(config)
  }
  import settings._

  lazy val client = dynamoClient(system, settings)

  implicit val timeout = Timeout(5.seconds)

  val schema = CreateTableRequest.builder()
    .attributeDefinitions(
      AttributeDefinition.builder().attributeName(Key).attributeType("S").build(),
      AttributeDefinition.builder().attributeName(SequenceNr).attributeType("N").build(),
      AttributeDefinition.builder().attributeName(Timestamp).attributeType("N").build()).keySchema(
        KeySchemaElement.builder().attributeName(Key).keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName(SequenceNr).keyType(KeyType.RANGE).build()).localSecondaryIndexes(
          LocalSecondaryIndex.builder()
            .indexName(TimestampIndex)
            .keySchema(
              KeySchemaElement.builder().attributeName(Key).keyType(KeyType.HASH).build(),
              KeySchemaElement.builder().attributeName(Timestamp).keyType(KeyType.RANGE).build())
            .projection(
              Projection.builder().projectionType(ProjectionType.ALL).build()).build())

  def ensureSnapshotTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create = schema
      .tableName(Table)
      .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(read).writeCapacityUnits(write).build())
      .build()

    var names = Vector.empty[String]
    lazy val complete: ListTablesResponse => Future[Vector[String]] = aws =>
      if (aws.lastEvaluatedTableName() == null) Future.successful(names ++ aws.tableNames().asScala)
      else {
        names ++= aws.tableNames().asScala
        client
          .listTables(ListTablesRequest.builder().exclusiveStartTableName(aws.lastEvaluatedTableName).build())
          .flatMap(complete)
      }
    val list = client.listTables(ListTablesRequest.builder().build()).flatMap(complete)

    val setup = for {
      exists <- list.map(_ contains Table)
      _ <- {
        if (exists) Future.successful(())
        else client.createTable(create)
      }
    } yield exists
    val r = Await.result(setup, 5.seconds)
  }

  private val writerUuid = UUID.randomUUID.toString

  var nextSeqNr = 1
  def seqNr() = {
    val ret = nextSeqNr
    nextSeqNr += 1
    ret
  }
}
