package akka.persistence.dynamodb.journal

import akka.persistence.PersistentRepr
import akka.persistence.dynamodb.DynamoDBConfig
import com.amazonaws.services.dynamodbv2.model.AttributeValue

class ItemSizeVerifier(dynamoDBConfig: DynamoDBConfig) {
  import dynamoDBConfig._

  def verifyItemSizeDidNotReachThreshold(repr: PersistentRepr, eventData: AttributeValue, serializerId: AttributeValue, manifest: String): Unit = {

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

    val itemSize =
      keyLength +
        eventData.getB.remaining +
        serializerId.getN.getBytes.length +
        manifestLength +
        fieldLength

    if (itemSize > MaxItemSize) {
      throw new DynamoDBJournalRejection(s"MaxItemSize exceeded: $itemSize > $MaxItemSize")
    }
  }

}
