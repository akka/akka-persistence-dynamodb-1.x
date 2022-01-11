package akka.persistence.dynamodb.journal

import akka.actor._
import akka.persistence.JournalProtocol.{ WriteMessageSuccess, WriteMessages, WriteMessagesSuccessful }
import akka.persistence._
import akka.persistence.dynamodb.journal.JournalDynamoItemTTLSpec.ttlFieldName
import akka.persistence.dynamodb.{ N, S }
import akka.persistence.journal.JournalSpec
import akka.testkit._
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, GetItemRequest, GetItemResult }
import com.typesafe.config.ConfigFactory

import java.time.OffsetDateTime
import java.util.AbstractMap.SimpleEntry
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class JournalDynamoItemTTLSpec extends PluginSpec(JournalDynamoItemTTLSpec.config) with DynamoDBUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    client.shutdown()
  }

  implicit lazy val system: ActorSystem = ActorSystem("JournalSpec", config.withFallback(JournalSpec.config))

  def journal: ActorRef =
    extension.journalFor(null)

  "DynamoDBJournalRequests" should {
    s"insert the ttl field in the item added to dynamo" in {

      val seqNumber = 1

      sendMessage(seqNumber)

      val response: GetItemResult = getItemFromDynamo(s"${settings.JournalName}-P-$pid-${seqNumber / 100}", seqNumber)

      val expectedTTL = OffsetDateTime.now.plusDays(300).toEpochSecond

      assert(response.getItem.get(ttlFieldName).getN.toLong === expectedTTL +- 1)

    }
  }

  def getItemFromDynamo(itemKey: String, itemSort: Int): GetItemResult = {
    val eventualDynamoResponse = client.getItem(
      new GetItemRequest()
        .withTableName(settings.JournalTable)
        .withKey(
          new SimpleEntry[String, AttributeValue](Key, S(itemKey)),
          new SimpleEntry[String, AttributeValue](Sort, N(itemSort))))
    Await.result(eventualDynamoResponse, 5.seconds)
  }

  def sendMessage(seqNumber: Int): Any = {
    val message = immutable.Seq(AtomicWrite(PersistentRepr(
      payload       = "a-1",
      sequenceNr    = seqNumber,
      persistenceId = pid,
      sender        = TestProbe().ref,
      writerUuid    = writerUuid)))

    val probe = TestProbe()

    journal ! WriteMessages(message, probe.ref, actorInstanceId)

    probe.expectMsg(WriteMessagesSuccessful)

    probe.expectMsgPF() {
      case WriteMessageSuccess(m @ PersistentImpl(payload, 1, _, _, _, _, _, _), _) =>
        payload should be(s"a-1")
    }
  }
}

object JournalDynamoItemTTLSpec {

  val ttlFieldName = "ttl-field-name"

  val config = ConfigFactory.parseString(
    s"""
my-dynamodb-journal {
  dynamodb-item-ttl-config {
    field-name = "$ttlFieldName"
    ttl = 300d
  }
}
""").resolve.withFallback(ConfigFactory.load())

}