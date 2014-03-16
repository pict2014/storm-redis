/**
 * This is the main file of the topology.
 * For benchmarking purpose change the ICommiter bolt that is
 * Calculator/StatelessCalculator to plot the graphs.
 */


package storm.ubiquitous;

import storm.ubiquitous.spouts.KafkaSpoutTransaction;
import storm.ubiquitous.bolts.Extractor;
import storm.ubiquitous.bolts.Calculator;
import storm.ubiquitous.bolts.StatelessCalculator;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TopologyMain{
public static void main(String[] args) throws Exception {
    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("state-trial", "transaction-spout", new KafkaSpoutTransaction(), 2);
    /* Use StatelessCalculator in place of Calculator for making it stateless. For Benchmarking purpose */
    builder.setBolt("extractor", new Extractor(), 4)
            .shuffleGrouping("transaction-spout");
    builder.setBolt("calculator", new Calculator(),4)
	.shuffleGrouping("extractor");
    LocalCluster cluster = new LocalCluster();
    Config config = new Config();
    
    config.setDebug(true);
    config.setMaxSpoutPending(3);
    config.setNumWorkers(3);
    if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], config, builder.buildTopology());
      }
    else
            cluster.submitTopology("global-state-topology", config, builder.buildTopology());
             Thread.sleep(10000);
             cluster.shutdown();
    }
}
