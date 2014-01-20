package storm.ubiquitous.bolts;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import storm.ubiquitous.state.PersistentMap;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchCount extends BaseTransactionalBolt implements ICommitter, Serializable{
	/**
	 * 
	 * Committer Bolt
	 * @author aniket
	 */
	private static final long serialVersionUID = -2343991642735232104L;

	@SuppressWarnings("serial")
	public static class CountValue implements Serializable{
		    public Integer prev_count = null;
		    public int count = 0;
		    public BigInteger txid = null;
		    public long atid = 0L;
	}
	
	public static Map<String, CountValue> INMEMORYDB = new ConcurrentHashMap<String, CountValue>();;
	
	
	TransactionAttempt _id;
	BatchOutputCollector _collector;
	String name;
	Map<String, Integer> counters;
	Integer _count = 1;
	PersistentMap mapStore;

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			    BatchOutputCollector collector, TransactionAttempt id) {
		_id = id;
		this.counters = new HashMap<String, Integer>();
		_collector=collector;
		mapStore=new PersistentMap("localhost");
	}
   
	public void execute(Tuple tuple) {
		name = tuple.getString(1);
   	
		if(!counters.containsKey(name)){
		  counters.put(name, _count);
		}
	  
		else{
		  _count = counters.get(name) + 1;
		  counters.put(name, _count);
		}
   }    
   
	public void finishBatch() throws FailedException{
		
	   	for (String key : counters.keySet()) {
	        
		CountValue val = INMEMORYDB.get(key);
		CountValue newVal;

	       	if (val == null || !val.txid.equals(_id.getTransactionId())) {
	          
	    	   	newVal = new CountValue();
	    	   	newVal.txid = _id.getTransactionId();
	    	   	newVal.atid = _id.getAttemptId();
	          
	          if (val != null) {
	        	  
	        	  newVal.prev_count = val.count;
	        	  newVal.count = val.count;
	          	}
	          
	           newVal.count = newVal.count + counters.get(key);
	           INMEMORYDB.put(key, newVal);
	       	}
	        
	       	else {
	         
	    	   newVal = val;
	    	   System.out.println("Tuple: " +  key + " Txid: " + newVal.txid + " AttemptID: "+ newVal.atid + 
	    	   " is replayed.");
	       	}
	        	System.out.println("String: "+key+" NewCount: "+newVal.count+" PrevCount: "
	        	+newVal.prev_count+" Txid: "+_id.getTransactionId()+" AttemptID: "+ _id.getAttemptId());
	       
	       	_collector.emit(new Values(_id, key, newVal.count, newVal.prev_count));
	      }
	       	//Store State
	   	if(counters.size()>0)
	      	{
	    	  	try {
	    		  	mapStore.setState(_id.getTransactionId().toByteArray(), INMEMORYDB);	    		  
	    	  	}
	    	  	catch (IOException e) {
	    		  	e.printStackTrace();
	    	  	}
	      	}
	   	
   }    
   
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
     		declarer.declare(new Fields("id","name", "count"));
   }
}
 
