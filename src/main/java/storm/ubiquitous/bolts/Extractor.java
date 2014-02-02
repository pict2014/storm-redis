package storm.ubiquitous.bolts;

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.mongodb.DBObject;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("rawtypes")
public class Extractor extends BaseBatchBolt 
{

    private static final long serialVersionUID = 7510735576780935459L;
    Object _id;
    BatchOutputCollector _collector;
    String[] words;
    JSONArray emitArray = null;
	  

    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) 
    {
    	_collector = collector;
    	_id = id;
    }
    
    public void execute(Tuple tuple) 
    {
    	
    	try
	{
	    JSONParser parser=new JSONParser();
	    DBObject obj = (DBObject) tuple.getValue(0);
              
	    String topic = new String();
	    topic = obj.get("topic").toString();
              
              
	    obj = (DBObject)obj.get("raw"); 
	    obj = (DBObject)obj.get("statuses");
              
	    JSONArray array = null;
	    array = (JSONArray) parser.parse(obj.toString());
	    JSONObject emitobj = new JSONObject();
	    emitobj.put("topic", topic);
              
              
              
	    String tz = new String();
	    try
	    {
              
		for(int i=0 ; i < array.size();i++)
		{
		    JSONObject jobj =(JSONObject) array.get(i);
		    emitobj.put("tweet",jobj.get("text"));
	              	
		    //timezone logic
		    tz = ((JSONObject)jobj.get("user")).get("time_zone").toString();
		    if(tz == "None")
			tz = ((JSONObject)jobj.get("user")).get("location").toString();
		    if(tz == "None")
			tz = jobj.get("place").toString();
		    if(tz == "None")
			tz = "unknown";
	              		
		}
	    }
	    catch(Exception e)
	    {
		tz = "unknown";
            }
              
              
              
             
               	
	    emitobj.put("country",tz);
	    emitArray.add(emitobj);
              
              
        }
    	catch(Exception e)
    	{
	    System.out.println("UnknownHostException: "+e);	
    	}

    }

    public void finishBatch() 
    {
    	if(emitArray!=null)
	{
	    for(Object emitObj : emitArray)
	    {
		_collector.emit(new Values(_id, emitObj));
	    }
    	}
    	
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
    	declarer.declare(new Fields("id", "extracted"));
    }
    
  }
