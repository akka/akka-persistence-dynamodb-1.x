/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import akka.actor.ActorSystem
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.testkit._
import com.amazonaws.services.dynamodbv2.model._
import java.util.{ HashMap => JHMap }
import akka.persistence.dynamodb._

class RecoveryConsistencySpec
    extends TestKit(ActorSystem("FailureReportingSpec"))
    with ImplicitSender
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals
    with DynamoDBUtils
    with IntegSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    client.shutdown()
    system.terminate().futureValue
    super.afterAll()
  }

  override val persistenceId = "RecoveryConsistencySpec"
  lazy val journal           = Persistence(system).journalFor("")

  import settings._

  "DynamoDB Journal (Recovery)" must {

    val repetitions = 50
    val messages    = 20
    val writes      = (1 to messages).map(i => AtomicWrite(persistentRepr(f"a-$i%04d")))
    val probe       = TestProbe()

    for (i <- 1 to repetitions)
      s"not return intermediate values for the highest sequence number ($i of $repetitions)" in {
        journal ! Purge(persistenceId, testActor)
        expectMsg(Purged(persistenceId))
        journal ! WriteMessages(writes, testActor, 1)
        journal ! ReplayMessages(1, 0, Long.MaxValue, persistenceId, probe.ref)
        expectMsg(WriteMessagesSuccessful)
        (1 to messages).foreach(i => expectMsgType[WriteMessageSuccess].persistent.sequenceNr.toInt should ===(i))
        probe.expectMsg(RecoverySuccess(messages))
      }

    "only replay completely persisted AtomicWrites" in {
      val more =
        AtomicWrite((1 to 3).map(i => persistentRepr(f"b-$i"))) ::   // hole in the middle
        AtomicWrite((4 to 6).map(i => persistentRepr(f"b-$i"))) ::   // hole in the beginning
        AtomicWrite((7 to 9).map(i => persistentRepr(f"b-$i"))) ::   // no hole
        AtomicWrite((10 to 12).map(i => persistentRepr(f"b-$i"))) :: // hole in the end
        AtomicWrite((13 to 15).map(i => persistentRepr(f"b-$i"))) :: // hole in the end
        AtomicWrite(persistentRepr("c")) ::
        AtomicWrite((17 to 19).map(i => persistentRepr(f"d-$i"))) :: // hole in the end
        Nil
      journal ! WriteMessages(more, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (messages + 1 to messages + 19).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      Seq(2, 4, 12, 15, 19).foreach(i => delete(messages + i))

      journal ! ReplayMessages(0, Long.MaxValue, Long.MaxValue, persistenceId, testActor)
      for {
        i <- 1 to (messages + 19)
        if i <= messages || (i >= (messages + 7) && i <= (messages + 9)) || i == (messages + 16)
      } expectMsg(ReplayedMessage(generatedMessages(i)))
      expectMsg(RecoverySuccess(messages + 18))

      generatedMessages = generatedMessages.dropRight(1)
      nextSeqNr = messages + 19
    }

    "read correct highest sequence number even if a Sort=0 entry is lost" in {
      val start = messages + 19
      val end   = (start / 100 + 1) * 100
      val more  = (start to end).map(i => AtomicWrite(persistentRepr(f"e-$i")))
      journal ! WriteMessages(more, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (start to end).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      delete(end)

      journal ! ListAll(persistenceId, testActor)
      val ids = ((1L to (end - 1)).toSet -- Set[Long](2, 4, 12, 15).map(_ + messages)).toSeq.sorted
      expectMsg(ListAllResult(persistenceId, Set.empty, (1L to (end / 100)).map(_ * 100).toSet, ids))

      journal ! ReplayMessages(0, Long.MaxValue, 0, persistenceId, testActor)
      expectMsg(RecoverySuccess(end))
    }

    "not replay corrupted batch" in {
      val start = nextSeqNr
      val more1 = AtomicWrite((1 to 3).map(i => persistentRepr(f"f-$i"))) :: Nil

      journal ! WriteMessages(more1, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (start to (start + 2)).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      delete(start + 2)

      nextSeqNr -= 1
      generatedMessages = generatedMessages.dropRight(1)

      val more2 = AtomicWrite((4 to 6).map(i => persistentRepr(f"f-$i"))) :: Nil

      journal ! WriteMessages(more2, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      ((start + 2) to (start + 4)).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      journal ! ReplayMessages(start, Long.MaxValue, Long.MaxValue, persistenceId, testActor)
      ((start + 2) to (start + 4)).foreach(i => expectMsg(ReplayedMessage(generatedMessages(i))))
      expectMsg(RecoverySuccess(start + 4))
    }

    "not replay differently corrupted batch" in {
      val start = nextSeqNr
      val more1 = AtomicWrite((1 to 3).map(i => persistentRepr(f"g-$i"))) :: Nil

      journal ! WriteMessages(more1, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      (start to (start + 2)).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      delete(start + 1)
      delete(start + 2)

      nextSeqNr -= 2
      generatedMessages = generatedMessages.dropRight(2)

      val more2 = AtomicWrite((4 to 6).map(i => persistentRepr(f"g-$i"))) :: Nil

      journal ! WriteMessages(more2, testActor, 1)
      expectMsg(WriteMessagesSuccessful)
      ((start + 1) to (start + 3)).foreach(i => expectMsg(WriteMessageSuccess(generatedMessages(i), 1)))

      journal ! ReplayMessages(start, Long.MaxValue, Long.MaxValue, persistenceId, testActor)
      ((start + 1) to (start + 3)).foreach(i => expectMsg(ReplayedMessage(generatedMessages(i))))
      expectMsg(RecoverySuccess(start + 3))
    }

  }

  private def delete(num: Long) = {
    val key: Item = new JHMap
    key.put(Key, S(s"$JournalName-P-$persistenceId-${num / 100}"))
    key.put(Sort, N(num % 100))
    client.deleteItem(new DeleteItemRequest().withTableName(JournalTable).withKey(key)).futureValue
  }
}
