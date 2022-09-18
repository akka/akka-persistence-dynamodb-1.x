package akka.persistence.dynamodb.query.javadsl

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.stream.javadsl.Source
import akka.persistence.dynamodb.query.scaladsl
import akka.persistence.query.javadsl.{ CurrentEventsByPersistenceIdQuery, CurrentPersistenceIdsQuery }

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

/**
 * Java API: `akka.persistence.query.javadsl.ReadJournal` implementation for Dynamodb.
 *
 * It is retrieved with:
 * {{{
 * DynamoDBReadJournal queries =
 *   PersistenceQuery.get(system).getReadJournalFor(DynamoDBReadJournal.class, DynamoDBReadJournal.Identifier());
 * }}}
 *
 * Corresponding Scala API is in [[DynamoDBReadJournal]].
 *
 * Configuration settings can be defined in the configuration section with the
 * absolute path corresponding to the identifier, which is `"dynamodb-read-journal"`
 * for the default [[DynamoDBReadJournal#Identifier]]. See `reference.conf`.
 */
class DynamoDBReadJournal(scaladslReadJournal: scaladsl.DynamoDBReadJournal)
    extends akka.persistence.query.javadsl.ReadJournal
    //    with akka.persistence.query.javadsl.EventsByTagQuery
    with CurrentEventsByPersistenceIdQuery
    with CurrentPersistenceIdsQuery
    //with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery
    {

  /**
   * Same type of query as [[akka.persistence.query.javadsl.EventsByPersistenceIdQuery.eventsByPersistenceId]]
   * but the event stream is completed immediately when it reaches the end of
   * the results. Events that are stored after the query is completed are
   * not included in the event stream.
   *
   * Execution plan:
   * - a dynamodb <code>query</code> to get lowest sequenceNr
   * - a <code>query</code> per partition. Doing follow calls to get more pages if necessary.
   */
  def currentEventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope, NotUsed] =
    scaladslReadJournal.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava

  /**
   * Same type of query as [[akka.persistence.query.javadsl.PersistenceIdsQuery.persistenceIds()]] but the stream
   * is completed immediately when it reaches the end of the "result set". Persistent
   * actors that are created after the query is completed are not included in the stream.
   *
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[akka.persistence.dynamodb.query.scaladsl.CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  def currentPersistenceIds(): Source[String, NotUsed] =
    scaladslReadJournal.currentPersistenceIds().asJava

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>scan</code> will be performed. Results will be paged per 1 MB size.
   */
  def currentPersistenceIdsByPageScan(): Source[List[String], NotUsed] =
    scaladslReadJournal.currentPersistenceIdsByPageScan().map(_.toList).asJava

  /**
   * Persistence ids are returned page by page.
   * A dynamodb <code>query</code> will be performed against a Global Secondary Index 'persistence-ids-idx'.
   * See [[akka.persistence.dynamodb.query.scaladsl.CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest]]
   */
  def currentPersistenceIdsByPageQuery(): Source[List[String], NotUsed] =
    scaladslReadJournal.currentPersistenceIdsByPageQuery().map(_.toList).asJava

}
