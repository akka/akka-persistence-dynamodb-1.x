/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.persistence.dynamodb.journal

import akka.persistence.dynamodb.IntegSpec

import akka.actor.ActorSystem
import akka.persistence.JournalProtocol._
import akka.persistence._
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

trait SerializeAsync

case class TestAsyncMessage(sequence: Int) extends SerializeAsync

object PartialAsyncSerializationSpec {
  val config = ConfigFactory
    .parseString("""
      |akka.actor {
      |  serializers {
      |    test = "akka.persistence.dynamodb.journal.TestSerializer"
      |  }
      |  serialization-bindings {
      |    "akka.persistence.dynamodb.journal.SerializeAsync" = test
      |  }
      |}
    """.stripMargin)
    .withFallback(ConfigFactory.load())
}

class PartialAsyncSerializationSpec
    extends TestKit(ActorSystem("PartialAsyncSerializationSpec", PartialAsyncSerializationSpec.config))
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

  override val persistenceId = "PartialAsyncSerializationSpec"
  lazy val journal           = Persistence(system).journalFor("")

  import settings._

  "DynamoDB Journal (Serialization Test)" must {

    val messages = 20
    val writes = (1 to messages).map(i =>
      if (i % 2 == 0) {
        AtomicWrite(persistentRepr(TestAsyncMessage(i)))
      } else {
        AtomicWrite(persistentRepr(f"a-$i%04d"))
      })
    val probe = TestProbe()

    s"should successfully serialize using async and not async serializers" in {

      journal ! Purge(persistenceId, testActor)
      expectMsg(Purged(persistenceId))
      journal ! WriteMessages(writes, testActor, 1)
      journal ! ReplayMessages(1, 0, Long.MaxValue, persistenceId, probe.ref)
      expectMsg(WriteMessagesSuccessful)
      (1 to messages).foreach(i => {
        val msg = expectMsgType[WriteMessageSuccess]
        msg.persistent.sequenceNr.toInt should ===(i)
        if (i % 2 == 0) {
          msg.persistent.payload should ===(TestAsyncMessage(i))
        }
      })
      probe.expectMsg(RecoverySuccess(messages))
    }

  }

}
