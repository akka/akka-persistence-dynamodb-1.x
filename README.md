DynamoDBJournal for Akka Persistence
====================================

A replicated [Akka Persistence](http://doc.akka.io/docs/akka/2.4.0/scala/persistence.html) journal backed by
[Amazon DynamoDB](http://aws.amazon.com/dynamodb/).

- This plugin implements both a journal as well as a snapshot store,
- This includes a Akka Persistence Query plugin. However, this requires an additional GSI for efficient usage.

Supported versions:
- Scala: `2.12.x`, `2.13.x`
- Akka: `2.5.x+` and `2.6.x+`
- Java: `1.8+`

[![Join the chat at https://gitter.im/akka/akka-persistence-dynamodb](https://badges.gitter.im/akka/akka-persistence-dynamodb.svg)](https://gitter.im/akka/akka-persistence-dynamodb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/akka/akka-persistence-dynamodb.svg?branch=master)](https://travis-ci.org/akka/akka-persistence-dynamodb)

Installation
------------

This plugin is published to the Maven Central repository with the following names:

~~~
<dependency>
    <groupId>com.typesafe.akka</groupId>
    <artifactId>akka-persistence-dynamodb_2.13</artifactId>
    <version>1.3.0</version>
</dependency>
~~~

or for sbt users:

```sbt
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-dynamodb" % "1.3.0"
```

Configuration
-------------

### Journal
~~~
akka.persistence.journal.plugin = "my-dynamodb-journal"

my-dynamodb-journal = ${dynamodb-journal} # include the default settings
my-dynamodb-journal {                     # and add some overrides
    journal-table =  <the name of the table to be used>
    journal-name =  <prefix to be used for all keys stored by this plugin>
    aws-access-key-id =  <your key>
    aws-secret-access-key =  <your secret>
    endpoint =  "https://dynamodb.us-east-1.amazonaws.com" # or where your deployment is
}
~~~

For details on the endpoint URL please refer to the [DynamoDB documentation](http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region). There are many more settings that can be used for fine-tuning and adapting this journal plugin to your use-case, please refer to the [reference.conf](https://github.com/akka/akka-persistence-dynamodb/blob/master/src/main/resources/reference.conf) file.

Before you can use these settings you will have to create a table, e.g. using the AWS console, with the following schema:

  * a hash key of type String with name `par`
  * a sort key of type Number with name `num`

### Snapshot store
(**Since:** `1.1.0`; contributed by [@joost-de-vries](https://github.com/joost-de-vries))

~~~
akka.persistence.snapshot-store.plugin = "my-dynamodb-snapshot-store"

my-dynamodb-snapshot-store = ${dynamodb-snapshot-store} # include the default settings
my-dynamodb-snapshot-store {                     # and add some overrides
    snapshot-table =  <the name of the table to be used>
    journal-name =  <prefix to be used for all keys stored by this plugin>
    aws-access-key-id =  <your key, default is the same as journal>
    aws-secret-access-key =  <your secret, default is the same as journal>
    endpoint =  "https://dynamodb.us-east-1.amazonaws.com" # or where your deployment is, default is the same as journal
}
~~~

The table to create for snapshot storage has the schema:

* a hash key of type String with name `par`
* a sort key of type Number with name `seq`
* a sort key of type Number with name `ts`
* a local secondary index with name `ts-idx` that is an index on the combination of `par` and `ts`

The Dynamodb item of a snapshot [can be 400 kB](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-items). Using a binary serialisation format like ProtoBuf or Kryo will use that space most effectively.

### Read journal (Akka persistence query)
(**Since:** `1.3.0`; contributed by [@joost-de-vries](https://github.com/joost-de-vries))
See `CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest` how to create the Global Secondary Index that is required to query currentPersistenceIds
~~~
dynamodb-read-journal {
  # The name of the Global Secondary Index that is used to query currentPersistenceIds
  # see CreatePersistenceIdsIndex.createPersistenceIdsIndexRequest
  # persistence-ids-index-name: "persistence-ids-idx"
}
~~~
Storage Semantics
-----------------

DynamoDB only offers consistency guarantees for a single storage item—which corresponds to one event in the case of this Akka Persistence plugin. This means that any single event is either written to the journal (and thereby visible to later replays) or it is not. This plugin supports atomic multi-event batches nevertheless, by marking the contained events such that partial replay can be avoided (see the `idx` and `cnt` attributes in the storage format description below). Consider the following actions of a PersistentActor:

```scala
val events = List(<some events>)
if (atomic) {
  persistAll(events)(handler)
else {
  for (event <- events) persist(event)(handler)
}
```

In the first case a recovery will only ever see all of the events or none of them. This is also true if recovery is requested with an upper limit on the sequence number to be recovered to or a limit on the number of events to be replayed; the event count limit is applied before removing incomplete batch writes which means that the actual count of events received at the actor may be lower than the requested limit even if further events are available.

In the second case each event is treated in isolation and may or may not be replayed depending on whether it was persisted successfully or not.

Performance Considerations
--------------------------

This plugin uses the AWS Java SDK which means that the number of requests that can be made concurrently is limited by the number of connections to DynamoDB and by the number of threads in the thread-pool that is used by the AWS HTTP client. The default setting is 50 connections which for a deployment that is used from the same EC2 region allows roughly 5000 requests per second (where every persisted event batch is roughly one request). If a single ActorSystem needs to persist more than this number of events per second then you may want to tune the parameter

~~~
my-dynamodb-journal.aws-client-config.max-connections = <your value here>
~~~

Changing this number changes both the number of concurrent connections and the used thread-pool size.

Compatibility with pre-1.0 versions
-----------------------------------

The storage layout has been changed incompatibly for performance and correctness reasons, therefore events stored with the old plugin cannot be used with versions since 1.0.

Plugin Development
------------------

### Dev Setup

* Run `./docker-compose up` to download and start [Localstack](https://github.com/localstack/localstack/).
* Make sure that env variables from .env.test are exported `source .env.test`
* Now you are all set for running the test suite from `sbt`.
* In order to stop the DynamoDB and clean up execute `./docker-compose down`.

Please also read the [CONTRIBUTING.md](CONTRIBUTING.md) file.

### DynamoDB table structure discussion

The structure for journal storage in dynamodb has evolved over iterations of performance tuning. Most of these lessons were learned in creating the eventsourced dynamodb journal, but apply here as well.

##### Naive structure

When initially modelling journal storage in dynamo, it seems natural to use a simple structure similar to this

```
persistenceId : S : HashKey
sequenceNr    : N : RangeKey
payload       : B
```

This maps very well to the operations a journal needs to solve.

```
writeMessage      -> PutItem
deleteMessage     -> DeleteItem
replayMessages    -> Query by persistenceId, conditions and ordered by sequenceNr, ascending
highCounter       -> Query by persistenceId, conditions and ordered by sequenceNr, descending limit 1
```

However this layout suffers from scalability problems. Since the hash key is used to locate the data storage node, all writes for a single processor will go to the same DynamoDB node, which limits throughput and invites throttling, no matter the level of throughput provisioned for a table—the hash key just gets too hot. Also this limits replay throughput since you have to step through a sequence of queries, where you use the last processed item in query N for query N+1.

##### Higher throughput structure

With the following abbreviations:

~~~
P -> PersistentRepr
SH -> SequenceHigh
SL -> SequenceLow
~~~

we model PersistentRepr storage as

~~~
par = <journalName>-P-<persistenceId>-<sequenceNr / 100> : S : HashKey
num = <sequenceNr % 100>                                 : N : RangeKey
pay = <payload>                                          : B
idx = <atomic write batch index>                         : N (possibly absent)
cnt = <atomic write batch max index>                     : N (possibly absent)
~~~

High Sequence Numbers

~~~
par = <journalName>-SH-<persistenceId>-<(sequenceNr / 100) % sequenceShards> : S : HashKey
num = 0                                                                      : N : RangeKey
seq = <sequenceNr rounded down to nearest multiple of 100>                   : N
~~~

Low Sequence Numbers

~~~
par = <journalName>-SL-<persistenceId>-<(sequenceNr / 100) % sequenceShards> : S : HashKey
num = 0                                                                      : N : RangeKey
seq = <sequenceNr, not rounded>                                              : N
~~~

This is somewhat more difficult to code, but offers higher throughput possibilities. Notice that the items that hold the high and low sequence are sharded, rather than using a single item to store the counter. If we only used a single item, we would suffer from the same hot key problems as our first structure.

When writing an item we typically do not touch the high sequence number storage, only when writing an item with sort key `0` is this done. This implies that reading the highest sequence number will need to first query the sequence shards for the highest multiple of 100 and then send a `Query` for the corresponding P entry’s hash key to find the highest stored sort key number.

Using with Akka 2.5.x
---------------------

Please make sure to depend on all Akka artifacts (those with the artifact name begining with
`akka-*`) are depended on in the same version - as mixing versions is *not* legal. For example, if
you depend on Akka Persistence in `2.5.3`, make sure that Akka Streams and Actors are also depended
on in the same version. Please always use the latest patch version available (!).

Credits
-------

- Initial development was done by [Scott Clasen](https://github.com/sclasen/akka-persistence-dynamodb).
- Update to Akka 2.4 and further development up to version 1.0 was kindly sponsored by [Zynga Inc.](https://www.zynga.com/).
- The snapshot store and readjournal were contributed by [Joost de Vries](https://github.com/joost-de-vries)
- [Corey O'Connor](https://dogheadbone.com/)
- Ryan Means
- Jean-Luc Deprez
- Michal Janousek

Support
-------

This project is *community maintained*. The Lightbend subscription does not cover support for this project.
