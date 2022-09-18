package akka.persistence.dynamodb.query

import akka.actor.ActorSystem
import akka.persistence.JournalProtocol._
import akka.persistence._
import akka.persistence.dynamodb.journal.DynamoDBUtils
import akka.persistence.dynamodb.query.scaladsl.{ CreatePersistenceIdsIndex, DynamoDBReadJournal }
import akka.persistence.dynamodb.{ DynamoProvider, IntegSpec }
import akka.persistence.query.PersistenceQuery
import akka.stream.scaladsl.Sink
import akka.stream.{ Materializer, SystemMaterializer }
import akka.testkit._
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import java.util.UUID
import scala.concurrent.duration.DurationInt
import CurrentPersistenceIdsSpec.{ toPersistenceId, RichSeq }

class CurrentPersistenceIdsSpec
    extends TestKit(ActorSystem("CurrentPersistenceIdsSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils
    with IntegSpec
    with CreatePersistenceIdsIndex
    with ReadJournalSettingsProvider
    with DynamoProvider {
  override protected lazy val readJournalSettings: DynamoDBReadJournalConfig = DynamoDBReadJournalConfig()
  override implicit val patienceConfig: PatienceConfig                       = PatienceConfig(15.seconds)

  private val writerUuid                          = UUID.randomUUID.toString
  private implicit val materializer: Materializer = SystemMaterializer(system).materializer
  private lazy val journal                        = Persistence(system).journalFor("")
  private lazy val queries =
    PersistenceQuery(system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)

  "DynamoDB ReadJournal" must {
    val persistenceIds = (0 to 100).map(toPersistenceId)

    "query current persistence ids" in {
      persistEvents(persistenceIds)

      val currentPersistenceIds = queries.currentPersistenceIds().runWith(Sink.collection).futureValue.toSeq

      currentPersistenceIds.sorted shouldBe persistenceIds.sorted
    }

    "query current persistence ids alphabetically" in {
      val morePersistenceIds = (101 to 200).map(toPersistenceId)
      persistEvents(morePersistenceIds)

      val fromPersistenceId = toPersistenceId(19)
      val currentPersistenceIds = queries
        .currentPersistenceIdsAlphabeticallyByPageQuery(fromPersistenceId = Some(fromPersistenceId))
        .runWith(Sink.collection)
        .futureValue
        .flatten

      val expectedIds = (persistenceIds ++ morePersistenceIds).dropUntilAlphabetically(fromPersistenceId)
      currentPersistenceIds shouldBe expectedIds
    }
  }

  private def persistEvents(persistenceIds: Seq[String]): Unit = {
    val eventsPerActor = 0 to 5
    val writes = persistenceIds.map(
      persistenceId =>
        AtomicWrite(
          eventsPerActor.map(
            i =>
              PersistentRepr(
                payload = s"$persistenceId $i",
                sequenceNr = i,
                persistenceId = persistenceId,
                writerUuid = writerUuid))))

    writes.foreach { message =>
      journal ! WriteMessages(message :: Nil, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      eventsPerActor.foreach(_ => expectMsgType[WriteMessageSuccess])
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
    createPersistenceIdsIndex(true).futureValue
  }

  override def afterAll(): Unit = {
    dynamo.shutdown()
    queries.close()
    system.terminate().futureValue
    super.afterAll()
  }
}

object CurrentPersistenceIdsSpec {
  def toPersistenceId(i: Int) = f"CurrentPersistenceIdsSpec_$i%04d"

  implicit class RichSeq(val ids: Seq[String]) {
    var drop = true

    def dropUntilAlphabetically(until: String): Seq[String] =
      ids.foldLeft(Seq.empty[String]) { (acc, value) =>
        val newAcc = if (drop) acc else acc ++ Seq(value)
        if (value == until) {
          drop = false
        }
        newAcc
      }
  }
}
