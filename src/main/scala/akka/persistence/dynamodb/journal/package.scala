/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

import software.amazon.awssdk.services.dynamodb.model._

package object journal {

  // field names
  val Key = "par"
  val Sort = "num"
  val Payload = "pay"
  val SequenceNr = "seq"
  val AtomIndex = "idx"
  val AtomEnd = "cnt"

  /* PersistenceRepr fields
   sequence_nr and persistence_id extracted from the key
  */
  val PersistentId = "persistence_id"
  val WriterUuid = "writer_uuid"

  val Manifest = "manifest"

  val Event = "event"
  val SerializerId = "ev_ser_id"
  val SerializerManifest = "ev_ser_manifest"

  val KeyPayloadOverhead = 26 // including fixed parts of partition key and 36 bytes fudge factor

  import collection.JavaConverters._

  val schema = CreateTableRequest.builder()
    .keySchema(
      KeySchemaElement.builder().attributeName(Key).keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName(Sort).keyType(KeyType.RANGE).build())
    .attributeDefinitions(
      AttributeDefinition.builder().attributeName(Key).attributeType("S").build(),
      AttributeDefinition.builder().attributeName(Sort).attributeType("N").build())

}
