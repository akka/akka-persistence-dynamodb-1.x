/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.dynamodb.journal

import akka.actor.ActorRef
import akka.pattern.extended.ask
import akka.persistence.journal.JournalSpec
import akka.persistence.CapabilityFlag

import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.persistence.dynamodb.IntegSpec

object AsyncDynamoDBJournalSpec {

  val config = ConfigFactory
    .parseString("""
      |akka.actor {
      |  serializers {
      |    test = "akka.persistence.dynamodb.journal.TestSerializer"
      |  }
      |  serialization-bindings {
      |    "java.io.Serializable" = test
      |  }
      |}
    """.stripMargin)
    .withFallback(ConfigFactory.load())

}

class AsyncDynamoDBJournalSpec extends JournalSpec(AsyncDynamoDBJournalSpec.config) with DynamoDBUtils with IntegSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    client.shutdown()
    super.afterAll()
  }

  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    Await.result(journal ? (Purge(pid, _)), 5.seconds)
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
  }

  def supportsRejectingNonSerializableObjects = CapabilityFlag.on()
}
