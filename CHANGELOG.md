Changelog
=========

v 1.3.0
--------------------

* Implementation of Akka Persistence Query. Thanks [Joost de Vries](https://github.com/joost-de-vries)!
  See (Configuration)[#read-journal-akka-persistence-query) for required setup.

v 1.1.2 (v 1.2.0)
----------------------------

This was supposed to be released as `v1.2.0` but was released as `v1.1.2` to maven. Sorry about that!

* Use DynamoDB Query during journal replay - https://github.com/akka/akka-persistence-dynamodb/issues/106
* Correct issue [#98](https://github.com/akka/akka-persistence-dynamodb/issues/98)
  Please see [fixes in `reference.conf`](blob/master/src/main/resources/reference.conf) for a workaround for systems impacted by this issues.
* Depends on Akka 2.5.
* Adds Support for the Async Serializers - which enables the use of the plugin with Lightbend extensions [GDPR Addons](https://developer.lightbend.com/docs/akka-commercial-addons/current/gdpr/index.html)

Schema changes are required in order to support async serializers as we need to know what data deserializer to use for the data payload.
The data payload is stored in a dedicated `event` field. Going towards similar schema as [akka-persistence-cassandra](https://github.com/akka/akka-persistence-cassandra)

*Journal Plugin*
~~~
val Event = "event" -> PeristentRepr.payload
val SerializerId = "ev_ser_id" -> Serializer id used for serializing event above
val SerializerManifest = "ev_ser_manifest" -> Serializer manifest of the event above
val Manifest = "manifest" -> String manifest used for whole PeristentRepr

~~~

*Snapshot Plugin*
~~~
val SerializerId = "ser_id" -> Serializer used for serializing the snapshot payload
val SerializerManifest = "ser_manifest" -> String manifest of the snapshot payload
val PayloadData = "pay_data" -> the actual serialized data of the snapshot, need to distinguish between the old a new format
~~~
The existence of the old `val Payload = "pay"` field triggers old serialization. The new serialization doesn't Serialize theq
Snapshot wrapper class.


Both Journal and Snapshot checks the existence of new data fields first and switches the behaviour in order
to make the change backwards compatible.
