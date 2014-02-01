kafka-storm-redis
=====================

This project implements an approach towards implementing **_stateful-ness of bolts_** in [Storm](https://github.com/nathanmarz/storm "storm").

###The project uses the following open-source projects:###
*   [Kafka](https://kafka.apache.org/ "Kafka")
*   [Redis](http://redis.io/ "redis")

###Explaination###

The project can be divided into two parts. The first is the _Spout-part_ which handles replaying of messages and 
the second is the _Bolt-part_ which manages the intermediate-state of the main processing.

####Spout####
**_Kafka_** is used as the data source for the *spout*. This makes replaying of messages easy and handy.
And with kafka there's no need for the spout(of the topology) to keep track of the messages by itself.
Spout used here extends a BasePartitionedTrnsactionalSpout which implements an IPartitionedTransactionalSpout.
Thus, only the TransactionMetadata is to be defined by the user as per need. Re-emitting of messages becomes very easy.
The user can also specify the size of each batch and no. of partitions used.

####Bolts####
**_Redis_** is used as the inmemory database to store the **intermediate state** of the _bolts_.
This project builds abstractions for bolts with fault-tolerant state, so if a task dies and gets reassigned to another machine it still has its state. The tuple trees that are made incomplete due to the bolt task failure will time-out and the spout will be able to replay the source tuple for that tree. Tuples that have already successfully completed will not be replayed. So generally you keep any persistent state in a database, oftentimes doing something like waiting to ack() tuples until you've done a batch update to the database. Stateful bolts will just be a much more efficient way of keeping a large amount of state at hand in a bolt.
```java
public interface IPersistentMap(String serverURL) {
      public Object getState(byte[] key);
      public void setState(byte[] key, Object value);
} 
```
The first implementation will target amounts of state that can fit into memory, so re-initialization time won't be a concern. But once we look at storing much larger amount of state we will need to consider this point.
State of Bolts get persisted periodically in Redis. Redis is an in-memory database that persists on disk. The data model is key-value, but many different kind of values are supported: Strings, Lists, Sets, Sorted Sets, Hashes <http://redis.io>

##Dependencies##
The project uses many dependencies for **kafka** and **redis**.
All dependenices are provided as maven dependecies.

Kafka uses the following dependencies

```xml
	<dependencies>
		<dependency>
			<groupId>org.springframework</groupId>
      			<artifactId>spring-core</artifactId>
      			<version>3.2.4.RELEASE</version>
    		</dependency>

    		<dependency>
			<groupId>org.springframework</groupId>
			<artifactId>spring-context</artifactId>
      			<version>3.2.4.RELEASE</version>
    		</dependency>

    		<dependency>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka_2.9.2</artifactId>
      			<version>0.8.0</version>
    			</dependency>

    		<dependency>
			<groupId>javax.inject</groupId>
      			<artifactId>javax.inject</artifactId>
      			<version>1</version>
    		</dependency>

	        <dependency>
			<groupId>org.scala-lang</groupId>
      			<artifactId>scala-library</artifactId>
      			<version>2.9.2</version>
    		</dependency>

    		<dependency>
			<groupId>log4j</groupId>
      			<artifactId>log4j</artifactId>
      			<version>1.2.17</version>
    		</dependency>

    		<dependency>
			<groupId>com.101tec</groupId>
      			<artifactId>zkclient</artifactId>
      			<version>0.3</version>
    		</dependency>

		<dependency>
			<groupId>com.yammer.metrics</groupId>
			<artifactId>metrics-core</artifactId>
			<version>2.2.0</version>
			</dependency>     

        </dependencies>

```

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
Runtime failure,
Restore State on failure


