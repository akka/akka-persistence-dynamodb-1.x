package akka.persistence.dynamodb.snapshot

import akka.actor._
import akka.persistence.SnapshotProtocol.SaveSnapshot
import akka.persistence._
import akka.persistence.dynamodb.snapshot.SnapshotDynamoItemTTLSpec.ttlFieldName
import akka.persistence.dynamodb.{ N, S }
import akka.persistence.journal.JournalSpec
import akka.testkit._
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, GetItemRequest, GetItemResult }
import com.typesafe.config.ConfigFactory

import java.time.OffsetDateTime
import java.util.AbstractMap.SimpleEntry
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class SnapshotDynamoItemTTLSpec extends PluginSpec(SnapshotDynamoItemTTLSpec.config) with DynamoDBUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureSnapshotTableExists()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    client.shutdown()
  }

  implicit lazy val system: ActorSystem = ActorSystem("JournalSpec", config.withFallback(JournalSpec.config))

  def snapshotStore: ActorRef =
    extension.snapshotStoreFor(null)

  "DynamoDBJournalRequests" should {
    s"insert the ttl field in the item added to dynamo" in {

      val seqNumber = 1

      saveSnapshot(seqNumber)

      val itemKey = s"${settings.JournalName}-P-$pid"

      val response: GetItemResult = getSnapshotFromDynamo(itemKey, seqNumber)

      val expectedTTL = OffsetDateTime.now.plusDays(200).toEpochSecond

      assert(response.getItem.get(ttlFieldName).getN.toLong === expectedTTL +- 1)

    }
  }

  private def getSnapshotFromDynamo(itemKey: String, seqNumber: Int) = {
    val eventualDynamoResponse = client.getItem(
      new GetItemRequest()
        .withTableName(settings.Table)
        .withKey(
          new SimpleEntry[String, AttributeValue](Key, S(itemKey)),
          new SimpleEntry[String, AttributeValue](SequenceNr, N(seqNumber))))

    Await.result(eventualDynamoResponse, 5.seconds)
  }

  private def saveSnapshot(seqNumber: Int) = {
    val probe = TestProbe()
    val metadata = SnapshotMetadata(pid, seqNumber)
    snapshotStore.tell(SaveSnapshot(metadata, s"s-${seqNumber}"), probe.ref)
    probe.expectMsgPF() {
      case SaveSnapshotSuccess(md) =>
        md
    }
  }
}

object SnapshotDynamoItemTTLSpec {

  val ttlFieldName = "ttl-field-name"

  val config = ConfigFactory.parseString(
    s"""
my-dynamodb-snapshot-store {
  dynamodb-item-ttl-config {
    field-name = "$ttlFieldName"
    ttl = 200d
  }
}
""").resolve.withFallback(ConfigFactory.load())

}