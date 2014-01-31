StatefulBolts
=====================
This is an idea to build abstractions for bolts with fault-tolerant state, so if a task dies and gets reassigned to another machine it still has its state. The tuple trees that are made incomplete due to the bolt task failure will time-out and the spout will be able to replay the source tuple for that tree. Tuples that have already successfully completed will not be replayed. So generally you keep any persistent state in a database, oftentimes doing something like waiting to ack() tuples until you've done a batch update to the database. Stateful bolts will just be a much more efficient way of keeping a large amount of state at hand in a bolt.
```java
public interface IPersistentMap(String serverURL) {
      public Object getState(byte[] key);
      public void setState(byte[] key, Object value);
} 
```
The first implementation will target amounts of state that can fit into memory, so re-initialization time won't be a concern. But once we look at storing much larger amount of state we will need to consider this point.

State of Bolts get persisted periodically in Redis. Redis is an in-memory database that persists on disk. The data model is key-value, but many different kind of values are supported: Strings, Lists, Sets, Sorted Sets, Hashes <http://redis.io>

Jedis is a Java client used for Redis, which can be used as a Maven dependency
```xml
 <!-- Jedis Dependency -->
	<dependency>
		<groupId>redis.clients</groupId>
		<artifactId>jedis</artifactId>
		<version>2.2.1</version>
		<type>jar</type>
		<scope>compile</scope>
	</dependency>
```
TODO
=====================
Kryo Serialization
Runtime failure
Restore State on failure


