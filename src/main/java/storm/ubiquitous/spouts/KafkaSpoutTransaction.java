/**
 * Transactional Spout with kafka acting as the source.
 * Takes messages from a single kafka topic and 
 * emits them as batches of tuples.
 * Topic can be divided into as many partitions as needed and
 * size of batch can be specified in bytes. Batches are of uniform
 * size.
 */

package storm.ubiquitous.spouts;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.HashMap;
import java.util.Map;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.topology.base.BasePartitionedTransactionalSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("rawtypes")
public class KafkaSpoutTransaction extends BasePartitionedTransactionalSpout<TransactionMetadata>{

    @Override
	public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf,TopologyContext context){
	return new KafkaPartitionedCoordinator();
    }
    @Override
	public IPartitionedTransactionalSpout.Emitter<TransactionMetadata> getEmitter(Map conf,TopologyContext context){
	return new KafkaPartitionedEmitter();
    }

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer){
	declarer.declare(new Fields("txid","data"));
    }
    public static class KafkaPartitionedCoordinator implements IPartitionedTransactionalSpout.Coordinator{

	@Override
	public int numPartitions(){
	    return 2;
	}
	@Override
	public boolean isReady(){
	    return true;
	}
	@Override
	public void close(){
	}
    }
    public static class KafkaPartitionedEmitter implements IPartitionedTransactionalSpout.Emitter<TransactionMetadata>{
	//Specifies the size of each batch in bytes.
	public static int SIZE=1000;
	@Override
	    public TransactionMetadata emitPartitionBatchNew(TransactionAttempt tx,BatchOutputCollector collector,int partition,TransactionMetadata lastPartitionMeta){
	    //each partition will have many batches depending on the size of partition
	    //and size of every batch is same in size.
	    //specified by SIZE variable.

	    TransactionMetadata metadata = new TransactionMetadata();
	    KafkaConsumer consumer;
	    long startoffset=0L;
	    long endoffset=0L;
	    try{
		if(lastPartitionMeta == null){
		    //if lastPartitionMeta is null we start from offset 0.
		    consumer = new KafkaConsumer(partition,0L,SIZE);
		}
		else{ 
		    //if lastPartitionMeta is not null then we get the end offset
		    // and start from the offset next to endoffset.
		    consumer = new KafkaConsumer(partition,lastPartitionMeta.endoffset+1,SIZE);
		}
		ByteBufferMessageSet messageSet =  consumer.fetchdata();
		System.out.println("Inside emitpartitionbatchnew. Fetching partition "+partition);

		int flag = 0;
		for(MessageAndOffset messageAndOffset: messageSet) {
		    if(flag == 0){
			//to get the start offset of the batch.
			startoffset = messageAndOffset.offset();
			flag=1;
		    }
		    //at last iteration will contain the end offset of the batch.
		    endoffset = messageAndOffset.offset();
		    ByteBuffer payload = messageAndOffset.message().payload();
		    byte[] bytes = new byte[payload.limit()];
		    payload.get(bytes);
		    collector.emit(new Values(tx, new String(bytes,"UTF-8")));
		}

		System.out.println(" From offset "+startoffset+" to "+endoffset);

		//create another TransactionMetadata obj and return it to be stored by zookeeper
		//and to be used later for replay.
		metadata = new TransactionMetadata(partition,startoffset,endoffset);
	    }
	    catch(Exception e){
	    }
	    return metadata;	
	}
	@Override
	    public void emitPartitionBatch(TransactionAttempt tx,BatchOutputCollector collector,int partition,TransactionMetadata partitionMeta){
	    //Replays a batch from a partitoi complete partition use kafka and partition no to fetch the data 
	    //use kafka and partition no. to fetch the data.
	    KafkaConsumer consumer = new KafkaConsumer(partitionMeta.partition,partitionMeta.startoffset,SIZE);
	    try{
		ByteBufferMessageSet messageSet =  consumer.fetchdata();
		System.out.println("Inside emitpartitionbatch. Fetching partition "+partitionMeta.partition+"From offset "+partitionMeta.startoffset+
				   " to "+partitionMeta.endoffset);

		for(MessageAndOffset messageAndOffset: messageSet) {
		    ByteBuffer payload = messageAndOffset.message().payload();
		    byte[] bytes = new byte[payload.limit()];
		    payload.get(bytes);
		    collector.emit(new Values(tx, new String(bytes,"UTF-8")));
		} 
	    }
	    catch(Exception e){
	    }
	}
	@Override
	    public void close(){
	}
    }
}
