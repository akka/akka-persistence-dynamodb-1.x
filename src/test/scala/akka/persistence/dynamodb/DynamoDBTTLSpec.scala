package akka.persistence.dynamodb

import org.scalatest.{ Matchers, WordSpec }

import java.time.{ Duration, OffsetDateTime }
import scala.concurrent.duration.DurationInt

class DynamoDBTTLSpec extends WordSpec with Matchers {

  "DynamoDBTTL#getItemExpiryTimeSeconds" should {

    "return the right number of seconds for if the ttl is 0" in {
      val now = OffsetDateTime.now()
      DynamoDBTTL(0.minutes).getItemExpiryTimeEpochSeconds(now) should be(now.toEpochSecond)
    }

    "return the right number of seconds for if the ttl is 5d" in {
      val now = OffsetDateTime.now()
      DynamoDBTTL(5.days).getItemExpiryTimeEpochSeconds(now) should be(now.plusDays(5).toEpochSecond)
    }

  }

  "DynamoDBTTL.fromJavaDuration" should {
    "convert from a java duration - 15min" in {
      DynamoDBTTL.fromJavaDuration(Duration.ofMinutes(15)).ttl should be(15.minutes)
    }
    "convert from a java duration - 7d" in {
      DynamoDBTTL.fromJavaDuration(Duration.ofDays(7)).ttl should be(7.days)
    }
  }

}
