/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.util.UUID
import akka.actor._
import akka.persistence._
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._
import akka.persistence.dynamodb.IntegSpec

/**
 * This class is pulled from https://github.com/krasserm/akka-persistence-cassandra/
 * @author https://github.com/krasserm
 */
object DynamoDBIntegrationLoadSpec {

  val config = ConfigFactory
    .parseString("""
my-dynamodb-journal {
  journal-table = "integrationLoadSpec"
  endpoint =  "http://localhost:8888"
  endpoint = ${?AWS_DYNAMODB_ENDPOINT}
  aws-access-key-id = "set something in case no real creds are there"
  aws-access-key-id = ${?AWS_ACCESS_KEY_ID}
  aws-secret-access-key = "set something in case no real creds are there"
  aws-secret-access-key = ${?AWS_SECRET_ACCESS_KEY}
}
akka.persistence.snapshot-store.plugin = ""
""").withFallback(ConfigFactory.load())
    .resolve()

  case class DeleteTo(snr: Long)

  class ProcessorAtomic(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
      case payload: List[_] =>
        persistAll(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        receiver ! recoveryRunning
    }
  }

  class ProcessorA(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        receiver ! recoveryRunning
    }
  }

  class ProcessorC(val persistenceId: String, probe: ActorRef) extends PersistentActor {
    var last: String = _

    def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot: String) =>
        last = snapshot
        probe ! s"offered-${last}"
      case payload: String =>
        handle(payload)
    }

    def receiveCommand: Receive = {
      case "snap" =>
        saveSnapshot(last)
      case SaveSnapshotSuccess(_) =>
        probe ! s"snapped-${last}"
      case payload: String =>
        persist(payload)(handle)
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
    }

    def handle: Receive = {
      case payload: String =>
        last = s"$payload-$lastSequenceNr"
        probe ! s"updated-$last"
    }
  }

  class ProcessorCNoRecover(override val persistenceId: String, probe: ActorRef, recoverConfig: Recovery)
      extends ProcessorC(persistenceId, probe) {
    override def recovery = recoverConfig

    override def preStart() = ()
  }

  class Listener extends Actor {
    def receive = {
      case d: DeadLetter => println(d)
    }
  }
}

import DynamoDBIntegrationLoadSpec._

class DynamoDBIntegrationLoadSpec
    extends TestKit(ActorSystem("test", config))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with DynamoDBUtils
    with IntegSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    client.shutdown()
    system.terminate()
    super.afterAll()
  }

  def subscribeToRangeDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessagesTo])

  def awaitRangeDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessagesTo]

  def testRangeDelete(persistenceId: String): Unit = {
    val deleteProbe = TestProbe()
    subscribeToRangeDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    (1L to 16L).foreach { i =>
      processor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    processor1 ! DeleteTo(3L)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    (4L to 16L).foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }

    processor1 ! DeleteTo(7L)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    (8L to 16L).foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  "A DynamoDB journal" should {

    "write and replay messages" in {
      val persistenceId = UUID.randomUUID().toString
      val processor1    = system.actorOf(Props(classOf[ProcessorA], persistenceId, self), "p1")
      (1L to 16L).foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self), "p2")
      (1L to 16L).foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }

      processor2 ! "b"
      expectMsgAllOf("b", 17L, false)
    }
    "not replay range-deleted messages" in {
      val persistenceId = UUID.randomUUID().toString
      testRangeDelete(persistenceId)
    }
    /* TODO Replace this with equivalent test

     "replay messages incrementally" in {
      val persistenceId = UUID.randomUUID().toString
      val probe = TestProbe()
      val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
      1L to 6L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val view = system.actorOf(Props(classOf[ViewA], "p7-view", persistenceId, probe.ref))
      probe.expectNoMsg(200.millis)

      view ! Update(await = true, replayMax = 3L)
      probe.expectMsg(s"a-1")
      probe.expectMsg(s"a-2")
      probe.expectMsg(s"a-3")
      probe.expectNoMsg(200.millis)

      view ! Update(await = true, replayMax = 3L)
      probe.expectMsg(s"a-4")
      probe.expectMsg(s"a-5")
      probe.expectMsg(s"a-6")
      probe.expectNoMsg(200.millis)
    } */
    "write and replay with persistAll greater than partition size skipping whole partition" in {
      val persistenceId   = UUID.randomUUID().toString
      val probe           = TestProbe()
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4", "a-5", "a-6")
      (1L to 6L).foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val testProbe  = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      (1L to 6L).foreach { i =>
        testProbe.expectMsgAllOf(s"a-${i}", i, true)
      }
    }
    "write and replay with persistAll greater than partition size skipping part of a partition" in {
      val persistenceId   = UUID.randomUUID().toString
      val probe           = TestProbe()
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3")
      (1L to 3L).foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      processorAtomic ! List("a-4", "a-5", "a-6")
      (4L to 6L).foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val testProbe  = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      (1L to 6L).foreach { i =>
        testProbe.expectMsgAllOf(s"a-${i}", i, true)
      }
    }
    "write and replay with persistAll less than partition size" in {
      val persistenceId   = UUID.randomUUID().toString
      val probe           = TestProbe()
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4")
      (1L to 4L).foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))
      (1L to 4L).foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }
    }
    "not replay messages deleted from the +1 partition" in {
      val persistenceId = UUID.randomUUID().toString
      val probe         = TestProbe()
      val deleteProbe   = TestProbe()
      subscribeToRangeDeletion(deleteProbe)
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      List("a-1", "a-2", "a-3", "a-4", "a-5", "a-6").foreach(processorAtomic ! List(_))
      (1L to 6L).foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }
      processorAtomic ! DeleteTo(5L)
      awaitRangeDeletion(deleteProbe)

      val testProbe  = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      testProbe.expectMsgAllOf(s"a-6", 6, true)
    }
  }
}
