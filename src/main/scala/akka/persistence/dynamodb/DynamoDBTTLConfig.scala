package akka.persistence.dynamodb

import com.typesafe.config.Config

import java.time.OffsetDateTime
import scala.concurrent.duration.{ Duration, DurationLong }
import scala.util.Try

case class DynamoDBTTLConfig(fieldName: String, ttl: DynamoDBTTL) {

  override def toString: String =
    s"DynamoDBTTLConfig(fieldName = ${fieldName}, ttl = ${ttl.ttl})"
}

case class DynamoDBTTL(ttl: Duration) {

  // Important, the value needs to be Unix epoch time format in seconds
  // See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/time-to-live-ttl-before-you-start.html#time-to-live-ttl-before-you-start-formatting
  def getItemExpiryTimeEpochSeconds(now: OffsetDateTime): Long =
    now.plusNanos(ttl.toNanos).toEpochSecond
}

object DynamoDBTTL {
  def fromJavaDuration(duration: java.time.Duration): DynamoDBTTL =
    DynamoDBTTL(duration.toNanos.nanos)
}

object DynamoDBTTLConfigReader {

  val configFieldName: String = "dynamodb-item-ttl-config.field-name"
  val configTtlName: String   = "dynamodb-item-ttl-config.ttl"

  def readTTLConfig(c: Config): Option[DynamoDBTTLConfig] = {
    for {
      fieldName <- Try(c.getString(configFieldName)).toOption.map(_.trim)
      if fieldName.nonEmpty
      ttl <- Try(c.getDuration(configTtlName)).toOption
    } yield DynamoDBTTLConfig(fieldName = fieldName, ttl = DynamoDBTTL.fromJavaDuration(ttl))
  }

}
