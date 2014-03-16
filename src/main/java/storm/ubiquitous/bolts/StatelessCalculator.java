/**
 * **Stateless**
 * Transactional bolt it is the last bolt that receives data from
 * from the first transactional bolt(Extractor) and saves the count
 * for each "trend-location" pair.
 */

package storm.ubiquitous.bolts;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.simple.JSONObject;

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

import storm.ubiquitous.ConfigProperties;

public class StatelessCalculator extends BaseTransactionalBolt implements ICommitter, Serializable{
	
    private static final long serialVersionUID = -2343991642735232104L;

    @SuppressWarnings("serial")
    public static class CountValue implements Serializable{
	CountValue(){
	    count = new HashMap<Object,Integer>();
	}
	public HashMap<Object,Integer> prev_count = null;
	public HashMap<Object,Integer> count ;
	public BigInteger txid = null;
	public long atid = 0L;
    }
	
    public static Map<Object, CountValue> INMEMORYDB = new ConcurrentHashMap<Object, CountValue>();
 
    TransactionAttempt _id;
    BatchOutputCollector _collector;
    Integer _count = 1;
    static int test;
    HashMap<Object, HashMap<Object,Integer>> counter; 
    HashMap<Object, Integer> h;
    MetricForBolt metric;
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,BatchOutputCollector collector, TransactionAttempt id){
	_id = id;
	_collector=collector;
	counter = new HashMap<Object,HashMap<Object,Integer>>();   
	metric = new MetricForBolt();
	metric.initializeMetricReporting();
    }
   
    public void execute(Tuple tuple){
		
	JSONObject jsonObj = (JSONObject) tuple.getValue(1);
	System.out.println("Calculator bolt. Msg "+jsonObj.toString());
        
        try{
	    h = counter.get(jsonObj.get("topic"));

	    Integer i = h.get(jsonObj.get("country"));
	    i++;
            
	    h.put(jsonObj.get("country"),i);
	    counter.put(jsonObj.get("topic"), h);
            	  
        }
        catch(Exception e){
	    h = new HashMap<Object,Integer>();
	    h.put(jsonObj.get("country"),1);
	    counter.put(jsonObj.get("topic"), h);
        }
	test++;

	//Failing deliberately
	if(test % 260 == 0){
	    metric.failedExceptions.mark();
	    throw new FailedException();
	}

    }    
   
    public void finishBatch() throws FailedException{
		
	for (Object key : counter.keySet()){
	        
	    CountValue val = INMEMORYDB.get(key);
	    CountValue newVal;
	          
		newVal = new CountValue();
		newVal.txid = _id.getTransactionId();
		newVal.atid = _id.getAttemptId();
		          
		if (val != null){
		    newVal.prev_count = val.count;
		    newVal.count = val.count;
		}
	
		newVal.count = counter.get(key);
		INMEMORYDB.put(key, newVal);
		       
	    _collector.emit(new Values(_id, key, newVal.count, newVal.prev_count));
	    metric.tuplesReceived.mark();
	}
    }    
   
    public void declareOutputFields(OutputFieldsDeclarer declarer){
	declarer.declare(new Fields("id","topic", "hash"));
    }
}
