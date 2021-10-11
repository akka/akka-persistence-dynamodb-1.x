package akka.persistence.dynamodb.journal

import akka.persistence.PersistentRepr
import akka.persistence.dynamodb._
import org.scalatest.{ Matchers, WordSpec }

import scala.concurrent.duration.DurationInt

class ItemSizeCalculatorSpec extends WordSpec with Matchers {
  val seqNumberSize = 1
  val serializerIdSize = 1

  "ItemSizeVerifier" should {

    "not include the size of the ttl field when it is not defined" in {
      getItemSize(None) should be(KeyPayloadOverhead + seqNumberSize + serializerIdSize)
    }

    "include the size of the ttl field when it is defined" in {
      val fieldName = "test"
      val dynamoDBTTLConfig = DynamoDBTTLConfig(fieldName, DynamoDBTTL(90.days))

      val seconds = "1633707331"

      getItemSize(Some(dynamoDBTTLConfig)) should be(KeyPayloadOverhead + seqNumberSize + serializerIdSize + fieldName.length + seconds.length)
    }
  }

  def getItemSize(someConfig: Option[DynamoDBTTLConfig]): Long = {
    val config = new DynamoDBConfig {
      override val AwsKey: String = ""
      override val AwsSecret: String = ""
      override val Endpoint: String = ""
      override val ClientDispatcher: String = ""
      override val client: ClientConfig = null
      override val Tracing: Boolean = false
      override val MaxBatchGet: Int = 1
      override val MaxBatchWrite: Int = 1
      override val MaxItemSize: Int = 400000
      override val Table: String = ""
      override val JournalName: String = ""
      override val TTLConfig: Option[DynamoDBTTLConfig] = someConfig
    }

    val repr = PersistentRepr.apply("")

    new ItemSizeCalculator(config)
      .getItemSize(
        repr         = repr,
        eventData    = B("".getBytes()),
        serializerId = N(0),
        manifest     = "")
  }

}
