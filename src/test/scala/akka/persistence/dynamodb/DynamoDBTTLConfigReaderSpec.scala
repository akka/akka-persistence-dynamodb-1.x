package akka.persistence.dynamodb

import com.typesafe.config.ConfigFactory
import org.scalatest.{ Matchers, WordSpec }

import scala.concurrent.duration.DurationInt

class DynamoDBTTLConfigReaderSpec extends WordSpec with Matchers {

  "DynamoDBTTLConfigReader.readTTLConfig" should {

    "read configuration" in {
      val config = ConfigFactory.parseString(
        """dynamodb-item-ttl-config {
          |  field-name = "ttl"
          |  ttl = 5d
          |}""".stripMargin)

      DynamoDBTTLConfigReader
        .readTTLConfig(config) should contain(
          DynamoDBTTLConfig("ttl", DynamoDBTTL(5.days)))
    }

    "return empty if the field name is defined but empty" in {
      val config = ConfigFactory.parseString(
        """dynamodb-item-ttl-config {
          |  field-name = ""
          |  ttl = 5d
          |}""".stripMargin)

      DynamoDBTTLConfigReader
        .readTTLConfig(config) should be(empty)
    }

    "return empty if the field name is defined but blank" in {
      val config = ConfigFactory.parseString(
        """dynamodb-item-ttl-config {
          |  field-name = " "
          |  ttl = 5d
          |}""".stripMargin)

      DynamoDBTTLConfigReader
        .readTTLConfig(config) should be(empty)
    }

    "return empty configuration if ttl is not defined" in {
      val config = ConfigFactory.parseString(
        """dynamodb-item-ttl-config {
          |  field-name = "asd"
          |  ttl = ""
          |}""".stripMargin)

      DynamoDBTTLConfigReader
        .readTTLConfig(config) should be(empty)
    }

    "return empty if there is no configuration" in {
      DynamoDBTTLConfigReader
        .readTTLConfig(ConfigFactory.empty()) should be(empty)
    }

  }

}
