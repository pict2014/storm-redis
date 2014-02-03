/**
 * Transactional bolt it is the first bolt that receives data from
 * from spout(KafkaSpoutTransactional) and emits the data to the next
 * bolt(Calculator).
 */

package storm.ubiquitous.bolts;

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.parser.JSONParser;


@SuppressWarnings("rawtypes")
public class Extractor extends BaseBatchBolt{

    private static final long serialVersionUID = 7510735576780935459L;
    Object _id;
    BatchOutputCollector _collector;
    String[] words;
    JSONArray emitArray;
    JSONParser parser;	  

    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id){
    	_collector = collector;
    	_id = id;
	parser = new JSONParser();
	emitArray = new JSONArray();
    }
    
    public void execute(Tuple tuple){
	try{
	    String str =   (String) tuple.getValue(1);
	    try{
		//COnverting the string to JSON format.
		JSONObject obj  = (JSONObject) parser.parse(str);
		System.out.println("Extractor bolt. Msg "+obj.toString());  

		//Creating a JSON object inserting the values and 
		//putting the JSON obj into a JSON Array
		JSONObject emitobj = new JSONObject();
		emitobj.put("topic", obj.get("trend"));
		emitobj.put("country",obj.get("location"));
		emitArray.add(emitobj);
	    }
	    catch (ParseException e){
		e.printStackTrace();
	    }            
              
	}
    	catch(Exception e){
	    System.out.println("UnknownHostException: "+e);	
	}

    }

    public void finishBatch(){
    	if(emitArray!=null){
	    for(Object emitObj : emitArray){
		_collector.emit(new Values(_id, emitObj));
	    }
	}
    	
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer){
    	declarer.declare(new Fields("id", "extracted"));
    }
}
