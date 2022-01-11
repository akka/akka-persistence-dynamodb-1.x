/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

package object snapshot {

  // field names
  val Key            = "par"
  val Timestamp      = "ts"
  val TimestampIndex = "ts-idx"

  val Payload            = "pay"
  val SequenceNr         = "seq"
  val SerializerId       = "ser_id"
  val SerializerManifest = "ser_manifest"
  val PayloadData        = "pay_data"
}
