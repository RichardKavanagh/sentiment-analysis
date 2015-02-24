package topology;

import spout.LogStashSpout;
import spout.ThreadPoolServer;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.JoinSentimentsBolt;
import bolts.NegativeWordsBolt;
import bolts.PositiveWordsBolt;
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
		
		builder.setBolt("positive_bag_of_words", new PositiveWordsBolt())
			.shuffleGrouping("sanitizer");
			
		builder.setBolt("negative_bag_of_words", new NegativeWordsBolt())
			.shuffleGrouping("sanitizer");
		
		builder.setBolt("positive_negative_join", new JoinSentimentsBolt())
			.fieldsGrouping("positive_bag_of_words", new Fields("tweet_id"))
			.fieldsGrouping("negative_bag_of_words", new Fields("tweet_id"));
		
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