package storm.ubiquitous;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.ubiquitous.bolts.BatchCount;
import storm.ubiquitous.bolts.BatchNormalizer;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * This is a basic example of a Transactional topology. It divides the sentence<tuple> stream  into words and 
 * keeps a count of the number of words seen so far in a database. The source of data and the databases are 
 * mocked out as in memory maps for demonstration purposes.
 * The state of Bolt is periodically stored in Redis, Which is an in-memory database that persists on disk.
 * @author aniket
 */
@SuppressWarnings("deprecation")
public class TransactionalGlobalCount {
  public static final int PARTITION_TAKE_PER_BATCH = 3;
  
  	@SuppressWarnings("serial")
	public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
    
	put(0, new ArrayList<List<Object>>() {{
      add(new Values("a b"));
      add(new Values("c d"));
      add(new Values("e f"));
      add(new Values("g h"));
      add(new Values("i j k l m"));
      add(new Values("o n p q r"));
      add(new Values("s t u v w"));
      add(new Values("x y z"));
      add(new Values("a1 b1"));
      add(new Values("c1 d1"));
      add(new Values("e1 f1"));
      add(new Values("g1 h1"));
      
    }});
    
	put(1, new ArrayList<List<Object>>() {{
    	 add(new Values("what are you doing aniket"));
         add(new Values("what are you doing abhishek"));
         add(new Values("what are you doing onkar"));
         add(new Values("what are you doing mohit"));
    }});
    
	put(2, new ArrayList<List<Object>>() {{
    	add(new Values("bye aniket"));
        add(new Values("bye abhishek"));
        add(new Values("bye onkar"));
        add(new Values("bye mohit"));
    }});
  }}; 
 
  public static void main(String[] args) throws Exception {
    MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("line"), PARTITION_TAKE_PER_BATCH);
    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 4);
    
    builder.setBolt("normalizer", new BatchNormalizer(), 5)
    	.shuffleGrouping("spout");
    builder.setBolt("inmemory-count", new BatchCount(), 1)
    	.fieldsGrouping("normalizer", new Fields("word"));
       
    LocalCluster cluster = new LocalCluster();
    Config config = new Config();
    
    //config.setDebug(true);
    config.setMaxSpoutPending(3);
    config.setNumWorkers(3);
    if (args != null && args.length > 0) {
    	StormSubmitter.submitTopology(args[0], config, builder.buildTopology());
      }
    else
    	cluster.submitTopology("global-count-topology", config, builder.buildTopology());
     	//Thread.sleep(1000);
     	//cluster.shutdown();
    }
}
