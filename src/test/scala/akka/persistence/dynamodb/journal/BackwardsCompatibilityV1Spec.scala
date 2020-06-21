/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.dynamodb.journal

import java.net.URI
import java.util
import java.util.Base64

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import akka.actor.ActorSystem
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.testkit._
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb._
import software.amazon.awssdk.services.dynamodb.model._

import com.typesafe.config.ConfigFactory

import akka.persistence.dynamodb._

class BackwardsCompatibilityV1Spec extends TestKit(ActorSystem("PartialAsyncSerializationSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils {

  def loadV1VersionData(): Unit = {
    val config = ConfigFactory.load()
    val endpoint = config.getString("my-dynamodb-journal.endpoint")
    val tableName = config.getString("my-dynamodb-journal.journal-table")
    val accesKey = config.getString("my-dynamodb-journal.aws-access-key-id")
    val secretKey = config.getString("my-dynamodb-journal.aws-secret-access-key")

    /*
    val client: DynamoDbAsyncClient = DynamoDbAsyncClient.builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accesKey, secretKey)))
      .region(Region.US_EAST_1)
      .endpointOverride(new URI(endpoint))
      .build()

     */

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

    def createItem(number: Int, data: String): Unit = {
      val item: Item = new util.HashMap()
      item.put(Key, S(persistenceId))
      item.put(Sort, N(number))
      item.put(Payload, B(Base64.getDecoder.decode(data)))

      client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
    }

    for {
      i <- messagePayloads.indices
      payload = messagePayloads(i)
    } yield createItem(i + 1, payload)

  }

  override def beforeAll(): Unit = {
    ensureJournalTableExists()
    loadV1VersionData()
  }

  override def afterAll(): Unit = {
    client.shutdown()
    system.terminate().futureValue
  }

  override val persistenceId = "OldFormatEvents"
  lazy val journal = Persistence(system).journalFor("")

  import settings._

  "DynamoDB Journal (Backwards Compatibility Test)" must {

    val messages = 20
    val probe = TestProbe()

    s"successfully replay events in old format - created by old version of the plugin" in {

      journal ! ReplayMessages(0, 20, Long.MaxValue, persistenceId, probe.ref)
      (1 to messages) foreach (i => {
        val msg = probe.expectMsgType[ReplayedMessage]
        msg.persistent.sequenceNr.toInt should ===(i)
        msg.persistent.payload should ===(f"a-$i%04d")
      })
      probe.expectMsg(RecoverySuccess(messages))
    }

  }

}
