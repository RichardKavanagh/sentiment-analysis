package topology;

import spout.LogStashSpout;
import spout.ThreadPoolServer;
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
		launchServer();
 
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

	private static void launchServer() {
		while(true){
			ThreadPoolServer server = new ThreadPoolServer(7777, 10);
			server.run();
		}
	}
}