/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.snapshot

import java.util.UUID

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.util.Timeout
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import akka.persistence.dynamodb.dynamoClient

trait DynamoDBUtils {

  def system: ActorSystem
  implicit val executionContext=system.dispatcher

  lazy val settings: DynamoDBSnapshotConfig = {
    val c = system.settings.config
    val config = c.getConfig(c.getString("akka.persistence.snapshot-store.plugin"))
    new DynamoDBSnapshotConfig(config)
  }
  import settings._

  lazy val client = dynamoClient(system, settings)

  implicit val timeout = Timeout(5.seconds)

  import com.amazonaws.services.dynamodbv2.model.{ KeySchemaElement, KeyType }

  val schema = new CreateTableRequest()
    .withAttributeDefinitions(
      new AttributeDefinition().withAttributeName(Key).withAttributeType("S"),
      new AttributeDefinition().withAttributeName(SequenceNr).withAttributeType("N"),
      new AttributeDefinition().withAttributeName(Timestamp).withAttributeType("N")
    )
    .withKeySchema(
      new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(SequenceNr).withKeyType(KeyType.RANGE)
    )
    .withLocalSecondaryIndexes(
      new LocalSecondaryIndex()
        .withIndexName(TimestampIndex).withKeySchema(
        new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
        new KeySchemaElement().withAttributeName(Timestamp).withKeyType(KeyType.RANGE)
      ).withProjection(new Projection().withProjectionType(ProjectionType.ALL))
    )

  def ensureSnapshotTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create = schema
      .withTableName(SnapshotTable)
      .withProvisionedThroughput(new ProvisionedThroughput(read, write))

    var names = Vector.empty[String]
    lazy val complete: ListTablesResult => Future[Vector[String]] = aws =>
      if (aws.getLastEvaluatedTableName == null) Future.successful(names ++ aws.getTableNames.asScala)
      else {
        names ++= aws.getTableNames.asScala
        client
          .listTables(new ListTablesRequest().withExclusiveStartTableName(aws.getLastEvaluatedTableName))
          .flatMap(complete)
      }
    val list = client.listTables(new ListTablesRequest).flatMap(complete)

    val setup = for {
      exists <- list.map(_ contains SnapshotTable)
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
  var generatedMessages: Vector[PersistentRepr] = Vector(null) // we start counting at 1

}
