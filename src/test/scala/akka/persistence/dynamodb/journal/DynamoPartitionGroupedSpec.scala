package akka.persistence.dynamodb.journal

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, Materializer, SystemMaterializer }
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.WordSpecLike

class DynamoPartitionGroupedSpec extends TestKit(ActorSystem("DynamoPartitionGroupedSpec")) with WordSpecLike {
  implicit val materializer: Materializer = SystemMaterializer(system).materializer

  assert(PartitionSize == 100, "This test is only valid with PartitionSize == 100")

  "A DynamoPartitionGroup should create the correct PartitionKey outputs" when {
    "events 1 thru 250 are presented" in {
      val sourceUnderTest =
        Source(1L to 250).via(DynamoPartitionGrouped)

      sourceUnderTest
        .runWith(TestSink.probe[PartitionKeys])
        .request(1)
        .expectNext(PartitionKeys(0L, 1L to 99))
        .request(1)
        .expectNext(PartitionKeys(1L, 100L to 199))
        .request(1)
        .expectNext(PartitionKeys(2L, 200L to 250))
        .expectComplete()
    }

    "events 85 through 300 are presented" in {
      val sourceUnderTest =
        Source(85L to 300).via(DynamoPartitionGrouped)

      sourceUnderTest
        .runWith(TestSink.probe[PartitionKeys])
        .request(1)
        .expectNext(PartitionKeys(0L, 85L to 99))
        .request(1)
        .expectNext(PartitionKeys(1L, 100L to 199))
        .request(1)
        .expectNext(PartitionKeys(2L, 200L to 299))
        .request(1)
        .expectNext(PartitionKeys(3L, scala.collection.immutable.Seq(300L)))
        .expectComplete()
    }

    "events 185 through 512 are presented" in {
      val sourceUnderTest =
        Source(185L to 512).via(DynamoPartitionGrouped)

      sourceUnderTest
        .runWith(TestSink.probe[PartitionKeys])
        .request(1)
        .expectNext(PartitionKeys(1L, 185L to 199))
        .request(3)
        .expectNext(PartitionKeys(2L, 200L to 299), PartitionKeys(3L, 300L to 399), PartitionKeys(4L, 400L to 499))
        .request(1)
        .expectNext(PartitionKeys(5L, 500L to 512))
        .expectComplete()
    }
  }

  "A DynamoPartitionGroup should complete when source is exhausted" when {
    "should complete with correct partition from events 1 through 15" in {
      val sourceUnderTest =
        Source(1L to 15).via(DynamoPartitionGrouped)

      sourceUnderTest
        .runWith(TestSink.probe[PartitionKeys])
        .request(2)
        .expectNext(PartitionKeys(0L, 1L to 15))
        .expectComplete()
    }

    "should complete with correct partition keys when events 1 through 99 are presented and source ends" in {
      val sourceUnderTest =
        Source(1L to 99).via(DynamoPartitionGrouped)

      sourceUnderTest
        .runWith(TestSink.probe[PartitionKeys])
        .request(2)
        .expectNext(PartitionKeys(0L, 1L to 99))
        .expectComplete()
    }
  }
}
