package topology;

import org.apache.log4j.Logger;

import spout.TwitterRiverSpout;
import utils.ThreadPoolServer;
import values.FieldValue;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.StreamSplitterBolt;
import bolts.SystemStatusBolt;
import bolts.TextPreProcessorBolt;
import bolts.TextSanitizerBolt;
import bolts.TweetEntityBolt;
import bolts.TweetInstanceBolt;
import bolts.TwitterFilterBolt;
import bolts.bag.JoinSentimentsBolt;
import bolts.bag.JoinedSentimentCalculatorBolt;
import bolts.bag.NegativeWordsBolt;
import bolts.bag.PositiveWordsBolt;
import bolts.bayes.BayesSentimentBolt;
import bolts.bayes.BayesSentimentCalculatorBolt;
import bolts.elasticsearch.ElasticSearchConfigurationBolt;
import bolts.elasticsearch.ElasticSearchWriterBolt;
import bolts.nlp.NLPSentimentBolt;
import bolts.nlp.NLPSentimentCalculatorBolt;

/*
 * Storm topology to perform sentiment analysis on a twitter stream.
 * 
 * @author Richard Kavanagh.
 */
public class SentimentAnalysisTopology {
	
	private static final Logger LOGGER = Logger.getLogger(SentimentAnalysisTopology.class);

	private static String TOPOLOGY_NAME = "SENTIMENT_ANALYSIS";
	private static int NUM_WORKERS = 1;
	
	public static void main(String[] args)  {

		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		
		setConfig(conf);
		createTopology(builder);

		if (validCMDLineArgs(args)) {
			LOGGER.info("Deploying remote storm topology.");
			conf.setNumWorkers(NUM_WORKERS);
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
			} catch (AlreadyAliveException err) {
				LOGGER.info("Topology already deployed." + err.getMessage());
			} catch (InvalidTopologyException err) {
				LOGGER.info("Invalid Topology." + err.getMessage());
			}
		}
		else {
			LOGGER.info("Deploying local Storm topology.");
			cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
			launchServer();
			cluster.killTopology(TOPOLOGY_NAME);
			cluster.shutdown();
		}
	}

	private static void createTopology(TopologyBuilder builder) {
		LOGGER.info("Creating Storm topology.");
		builder.setSpout("twitter_river_spout", new TwitterRiverSpout());

		builder.setBolt("elasticsearch_configuration", new ElasticSearchConfigurationBolt())
		.shuffleGrouping("twitter_river_spout");
		
		builder.setBolt("system_status", new SystemStatusBolt())
		.shuffleGrouping("elasticsearch_configuration");

		builder.setBolt("instance_filter", new TweetInstanceBolt())
		.shuffleGrouping("system_status");

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

		builder.setBolt("bayes_sentiment", new BayesSentimentBolt())
		.shuffleGrouping("stream_splitter", "bayesSentimentAnalysisStream");

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

		builder.setBolt("bayes_sentiment_calculator", new BayesSentimentCalculatorBolt())
		.shuffleGrouping("bayes_sentiment");

		builder.setBolt("elasticsearch_writer", new ElasticSearchWriterBolt())
		.shuffleGrouping("stream_splitter", "elasticSearchStream")
		.fieldsGrouping("entity_parser", new Fields(FieldValue.URL.getString(), FieldValue.LOCATION.getString()))
		.fieldsGrouping("sentiment_calculator", new Fields(FieldValue.SENTIMENT.getString()))
		.fieldsGrouping("nlp_sentiment_calculator", new Fields(FieldValue.SENTIMENT.getString()))
		.fieldsGrouping("bayes_sentiment_calculator", new Fields(FieldValue.SENTIMENT.getString()));
	}
	
	private static void setConfig(Config conf) {
		conf.setDebug(false);
	}

	private static boolean validCMDLineArgs(String [] args) {
		return args != null && args.length > 0;
	}

	private static void launchServer() {
		final int port = 7777, threads = 10;
		while(true) {
			ThreadPoolServer server = new ThreadPoolServer(port, threads);
			server.run();
		}
	}
}