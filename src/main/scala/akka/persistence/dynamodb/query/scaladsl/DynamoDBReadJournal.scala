package akka.persistence.dynamodb.query.scaladsl

/**
 * Scala API `akka.persistence.query.scaladsl.ReadJournal` implementation for DynamoDB.
 *
 * It is retrieved with:
 * {{{
 * val queries = PersistenceQuery(system).readJournalFor[DynamoDBReadJournal](DynamoDBReadJournal.Identifier)
 * }}}
 *
 * Corresponding Java API is in [[akka.persistence.dynamodb.query.javadsl.DynamoDBReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"dynamodb-read-journal"`
 * for the default [[DynamoDBReadJournal#Identifier]]. See `reference.conf`.
 */
trait DynamoDBReadJournal extends DynamoDBCurrentEventsByPersistenceIdQuery with DynamoDBCurrentPersistenceIdsQuery {
  def close(): Unit
}

object DynamoDBReadJournal {

  /**
   * The default identifier for [[DynamoDBReadJournal]] to be used with
   * `akka.persistence.query.PersistenceQuery#readJournalFor`.
   *
   * The value is `"dynamodb-read-journal"` and corresponds
   * to the absolute path to the read journal configuration entry.
   */
  val Identifier = "dynamodb-read-journal"
}
