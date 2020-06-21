/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import software.amazon.awssdk.services.dynamodb.model._
import scala.concurrent.Await
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.util.Timeout
import java.util.UUID
import akka.persistence.PersistentRepr
import scala.collection.JavaConverters._
import akka.persistence.dynamodb._

trait DynamoDBUtils {

  val system: ActorSystem
  import system.dispatcher

  lazy val settings = {
    val c = system.settings.config
    val config = c.getConfig(c.getString("akka.persistence.journal.plugin"))
    new DynamoDBJournalConfig(config)
  }
  import settings._

  lazy val client = dynamoClient(system, settings)

  implicit val timeout = Timeout(5.seconds)

  def ensureJournalTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create = schema
      .tableName(JournalTable)
      .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(read).writeCapacityUnits(write).build())
      .build()

    var names = Vector.empty[String]
    lazy val complete: ListTablesResponse => Future[Vector[String]] = aws =>
      if (aws.lastEvaluatedTableName() == null) Future.successful(names ++ aws.tableNames().asScala)
      else {
        names ++= aws.tableNames().asScala
        client
          .listTables(ListTablesRequest.builder().exclusiveStartTableName(aws.lastEvaluatedTableName()).build())
          .flatMap(complete)
      }
    val list = client.listTables(ListTablesRequest.builder().build()).flatMap(complete)

    val setup = for {
      exists <- list.map(_ contains JournalTable)
      _ <- {
        if (exists) Future.successful(())
        else client.createTable(create)
      }
    } yield ()
    Await.result(setup, 5.seconds)
  }

  private val writerUuid = UUID.randomUUID.toString
  def persistenceId: String = ???

  var nextSeqNr = 1
  def seqNr() = {
    val ret = nextSeqNr
    nextSeqNr += 1
    ret
  }
  var generatedMessages: Vector[PersistentRepr] = Vector(null) // we start counting at 1

  def persistentRepr(msg: Any) = {
    val ret = PersistentRepr(msg, sequenceNr = seqNr(), persistenceId = persistenceId, writerUuid = writerUuid)
    generatedMessages :+= ret
    ret
  }
}
