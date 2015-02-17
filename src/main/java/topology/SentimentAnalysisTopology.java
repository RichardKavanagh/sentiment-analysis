package topology;

import spout.LogStashSpout;
import spout.ThreadPoolServer;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolts.TextPreprocessorBolt;
import bolts.TextSanitizerBolt;
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

		builder.setSpout("logstash_spout", new LogStashSpout());
		
		builder.setBolt("twitter_filter", new TwitterFilterBolt())
			.shuffleGrouping("logstash_spout");
		builder.setBolt("preprocessor", new TextPreprocessorBolt())
			.shuffleGrouping("twitter_filter");
		builder.setBolt("sanitizer", new TextSanitizerBolt())
			.shuffleGrouping("preprocessor");
		
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
		launchServer();
 
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

	private static void launchServer() {
		final int port = 7777, threads = 10;
		while(true){
			ThreadPoolServer server = new ThreadPoolServer(port, threads);
			server.run();
		}
	}
}