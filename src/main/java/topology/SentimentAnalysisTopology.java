package topology;

import spout.LogStashSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolts.TwitterFilterBolt;

/*
 * Storm topology to perform sentiment analysis on a twitter stream.
 * 
 * @author Richard Kavanagh.
 */
public class SentimentAnalysisTopology {
	
    public static void main(String[] args)  {
    	
    	String TOPOLOGY_NAME = "SENTIMENT_ANALYSIS";
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("logstash", new LogStashSpout() );
        builder.setBolt("preprocessor", new TwitterFilterBolt() ).shuffleGrouping("logstash");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        stayAlive();
        
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

	private static void stayAlive() {
		while(true){
			try {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e) {}
		}
	}
}