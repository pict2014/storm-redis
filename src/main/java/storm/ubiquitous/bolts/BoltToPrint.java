/**
 * A bolt to verify the 
 * the working of KafkaSpoutTransaction
 */

package storm.ubiquitous.bolts;


import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("rawtypes")
public class BoltToPrint extends BaseBatchBolt {
    /**
     * @author aniket
     */
    private static final long serialVersionUID = 7510735576780935459L;
    Object _id;
    BatchOutputCollector _collector;
    String[] words;

    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
	_collector = collector;
	_id = id;
    }
    
    public void execute(Tuple tuple) {
	String sentence = tuple.getString(1);
	System.out.println("Inside BoltToPrint. Sentence "+sentence);
	words = sentence.split(" ");
    }

    public void finishBatch() {
	if(words!=null)
            {
		for(String word : words){
                    word = word.trim();
		    if(!word.isEmpty())                    
			_collector.emit(new Values(_id, word));
		}
            }
            
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("id", "word"));
    }
    
}
