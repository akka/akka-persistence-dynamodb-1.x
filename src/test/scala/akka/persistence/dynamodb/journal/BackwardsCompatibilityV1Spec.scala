/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.dynamodb.journal

import java.util.Base64

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import akka.actor.ActorSystem
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.testkit._
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClient }
import com.amazonaws.services.dynamodbv2.document.{ DynamoDB, Item }
import com.typesafe.config.ConfigFactory
import akka.persistence.dynamodb.IntegSpec

class BackwardsCompatibilityV1Spec
    extends TestKit(ActorSystem("PartialAsyncSerializationSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils
    with IntegSpec {

  def loadV1VersionData(): Unit = {
    val config    = ConfigFactory.load()
    val endpoint  = config.getString("my-dynamodb-journal.endpoint")
    val tableName = config.getString("my-dynamodb-journal.journal-table")
    val accesKey  = config.getString("my-dynamodb-journal.aws-access-key-id")
    val secretKey = config.getString("my-dynamodb-journal.aws-secret-access-key")

    val client: AmazonDynamoDB =
      new AmazonDynamoDBClient(new BasicAWSCredentials(accesKey, secretKey)).withRegion(Regions.US_EAST_1)

    client.setEndpoint(endpoint)

    val dynamoDB = new DynamoDB(client)

    val persistenceId = "journal-P-OldFormatEvents-0"

    val messagePayloads = Seq(
      "ChEIARINrO0ABXQABmEtMDAwMRABGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwMhACGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwMxADGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNBAEGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNRAFGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNhAGGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwNxAHGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwOBAIGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAwORAJGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMBAKGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMRALGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMhAMGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxMxANGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNBAOGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNRAPGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNhAQGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxNxARGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxOBASGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAxORATGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==",
      "ChEIARINrO0ABXQABmEtMDAyMBAUGg9PbGRGb3JtYXRFdmVudHNqJDI5NWZhMTE2LWZhOTUtNGY1Yi1hZjk2LTgwZDk4NjFhODk4ZA==")

    val table = dynamoDB.getTable(tableName)

    def createItem(number: Int, data: String): Unit = {
      table.putItem(
        new Item()
          .withPrimaryKey("par", persistenceId, "num", number)
          .withBinary("pay", Base64.getDecoder.decode(data)))
    }

    for {
      i <- messagePayloads.indices
      payload = messagePayloads(i)
    } yield createItem(i + 1, payload)

  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
    loadV1VersionData()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    client.shutdown()
    system.terminate().futureValue
  }

  override val persistenceId = "OldFormatEvents"
  lazy val journal           = Persistence(system).journalFor("")

  import settings._

  "DynamoDB Journal (Backwards Compatibility Test)" must {

    val messages = 20
    val probe    = TestProbe()

    s"successfully replay events in old format - created by old version of the plugin" in {

      journal ! ReplayMessages(0, 20, Long.MaxValue, persistenceId, probe.ref)
      (1 to messages).foreach(i => {
        val msg = probe.expectMsgType[ReplayedMessage]
        msg.persistent.sequenceNr.toInt should ===(i)
        msg.persistent.payload should ===(f"a-$i%04d")
      })
      probe.expectMsg(RecoverySuccess(messages))
    }

  }

}
