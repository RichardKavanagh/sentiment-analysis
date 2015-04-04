package topology;

import spout.TwitterRiverSpout;
import spout.ThreadPoolServer;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.ElasticSearchWriterBolt;
import bolts.JoinSentimentsBolt;
import bolts.JoinedSentimentCalculator;
import bolts.NegativeWordsBolt;
import bolts.PositiveWordsBolt;
import bolts.TextPreprocessorBolt;
import bolts.TextSanitizerBolt;
import bolts.TweetInstanceBolt;
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

		builder.setSpout("twitter_river_spout", new TwitterRiverSpout());

		builder.setBolt("instance_filter", new TweetInstanceBolt())
		.shuffleGrouping("twitter_river_spout");
		
		builder.setBolt("twitter_filter", new TwitterFilterBolt())
			.shuffleGrouping("instance_filter");
		
		builder.setBolt("preprocessor", new TextPreprocessorBolt())
			.shuffleGrouping("twitter_filter");
		
		builder.setBolt("sanitizer", new TextSanitizerBolt())
			.shuffleGrouping("preprocessor", "processeMessageStream");
		
		builder.setBolt("positive_bag_of_words", new PositiveWordsBolt())
			.shuffleGrouping("sanitizer");
			
		builder.setBolt("negative_bag_of_words", new NegativeWordsBolt())
			.shuffleGrouping("sanitizer");
		
		builder.setBolt("positive_negative_join", new JoinSentimentsBolt())
			.fieldsGrouping("positive_bag_of_words", new Fields("tweet_id"))
			.fieldsGrouping("negative_bag_of_words", new Fields("tweet_id"));
		
		builder.setBolt("sentiment_calculator", new JoinedSentimentCalculator())
			.shuffleGrouping("positive_negative_join");
		
		builder.setBolt("elasticsearch_writer", new ElasticSearchWriterBolt())
			.shuffleGrouping("preprocessor", "writeMessageStream")
			.fieldsGrouping("sentiment_calculator", new Fields("tweet_sentiment"));
		
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