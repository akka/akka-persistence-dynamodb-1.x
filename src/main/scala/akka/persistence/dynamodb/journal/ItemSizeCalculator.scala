package akka.persistence.dynamodb.journal

import akka.persistence.PersistentRepr
import akka.persistence.dynamodb.{ DynamoDBConfig, DynamoDBTTLConfig }
import com.amazonaws.services.dynamodbv2.model.AttributeValue

import java.time.OffsetDateTime

class ItemSizeCalculator(dynamoDBConfig: DynamoDBConfig) {
  import dynamoDBConfig._

  private val ttlInEpochSecondsLength = OffsetDateTime.now().toEpochSecond.toString.length

  def getItemSize(
      repr: PersistentRepr,
      eventData: AttributeValue,
      serializerId: AttributeValue,
      manifest: String): Long = {

    val fieldLength =
      repr.persistenceId.getBytes.length +
      repr.sequenceNr.toString.getBytes.length +
      repr.writerUuid.getBytes.length +
      repr.manifest.getBytes.length

    val manifestLength = if (manifest.isEmpty) 0 else manifest.getBytes.length

    val keyLength =
      repr.persistenceId.length +
      JournalName.length +
      KeyPayloadOverhead

    val ttlSize = TTLConfig
      .map {
        case DynamoDBTTLConfig(fieldName, _) => fieldName.length + ttlInEpochSecondsLength
      }
      .getOrElse(0)

    keyLength +
    eventData.getB.remaining +
    serializerId.getN.getBytes.length +
    manifestLength +
    fieldLength +
    ttlSize
  }

}
