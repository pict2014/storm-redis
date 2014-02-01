/**
 * Uses the kafka api to fetch messages from a kafka topic.
 * partition, offset and the size(in bytes) are expected 
 * to be provided by the caller. 
 * It is used by the KafkaTransactionSpout.
 */

package storm.ubiquitous.spouts;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumer implements KafkaProperties{
    
    private final Integer pno;
    private final Integer size;
    private final long offset;

    public  KafkaConsumer(Integer pno,long offset,Integer size){
	this.pno = new Integer(pno);
	this.offset = offset;
	this.size = size;
	System.out.println("thread for partition "+this.pno);
    }
   
    public ByteBufferMessageSet  fetchdata() throws Exception {
      
	SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
							   KafkaProperties.kafkaServerPort,
							   KafkaProperties.connectionTimeOut,
							   KafkaProperties.kafkaProducerBufferSize,
							   KafkaProperties.clientId);

	System.out.println("Fetching partition "+pno);
	FetchRequest req = new FetchRequestBuilder()
            .clientId(KafkaProperties.clientId)
            .addFetch(KafkaProperties.topic, pno, offset, size)
            .build();
	FetchResponse fetchResponse = simpleConsumer.fetch(req);
	return (ByteBufferMessageSet) fetchResponse.messageSet(KafkaProperties.topic, pno);

    }
}
