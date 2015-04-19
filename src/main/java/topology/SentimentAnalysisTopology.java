package topology;

import spout.TwitterRiverSpout;
import utils.ThreadPoolServer;
import values.FieldValue;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.ElasticSearchConfigurationBolt;
import bolts.ElasticSearchWriterBolt;
import bolts.JoinSentimentsBolt;
import bolts.JoinedSentimentCalculatorBolt;
import bolts.NegativeWordsBolt;
import bolts.PositiveWordsBolt;
import bolts.StreamSplitterBolt;
import bolts.TextPreProcessorBolt;
import bolts.TextSanitizerBolt;
import bolts.TweetEntityBolt;
import bolts.TweetInstanceBolt;
import bolts.TwitterFilterBolt;
import bolts.NLP.NLPSentimentBolt;
import bolts.NLP.NLPSentimentCalculatorBolt;

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
		
		builder.setBolt("elasticsearch_configuration", new ElasticSearchConfigurationBolt())
			.shuffleGrouping("twitter_river_spout");

		builder.setBolt("instance_filter", new TweetInstanceBolt())
		.shuffleGrouping("elasticsearch_configuration");
		
		builder.setBolt("twitter_filter", new TwitterFilterBolt())
			.shuffleGrouping("instance_filter");
		
		builder.setBolt("preprocessor", new TextPreProcessorBolt())
			.shuffleGrouping("twitter_filter");
		
		builder.setBolt("stream_splitter", new StreamSplitterBolt())
			.shuffleGrouping("preprocessor");
		
		builder.setBolt("sanitizer", new TextSanitizerBolt())
			.shuffleGrouping("stream_splitter", "sentimentAnalysisStream");
	
		builder.setBolt("entity_parser", new TweetEntityBolt())
			.shuffleGrouping("stream_splitter", "entityParseStream");
		
		builder.setBolt("nlp_sentiment", new NLPSentimentBolt())
			.shuffleGrouping("stream_splitter", "nlpSentimentAnalysisStream");
		
		builder.setBolt("positive_bag_of_words", new PositiveWordsBolt())
			.shuffleGrouping("sanitizer");
			
		builder.setBolt("negative_bag_of_words", new NegativeWordsBolt())
			.shuffleGrouping("sanitizer");
		
		builder.setBolt("positive_negative_join", new JoinSentimentsBolt())
			.fieldsGrouping("positive_bag_of_words", new Fields(FieldValue.POSITIVE.getString()))
			.fieldsGrouping("negative_bag_of_words", new Fields(FieldValue.NEGATIVE.getString()));
		
		builder.setBolt("sentiment_calculator", new JoinedSentimentCalculatorBolt())
			.shuffleGrouping("positive_negative_join");
		
		builder.setBolt("nlp_sentiment_calculator", new NLPSentimentCalculatorBolt())
			.shuffleGrouping("nlp_sentiment");
		
		builder.setBolt("elasticsearch_writer", new ElasticSearchWriterBolt())
			.shuffleGrouping("stream_splitter", "elasticSearchStream")
			.fieldsGrouping("entity_parser", new Fields(FieldValue.URL.getString(), FieldValue.LOCATION.getString()))
			.fieldsGrouping("sentiment_calculator", new Fields(FieldValue.SENTIMENT.getString()))
			.fieldsGrouping("nlp_sentiment_calculator", new Fields(FieldValue.SENTIMENT.getString()));
		
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