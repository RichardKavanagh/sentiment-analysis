package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import topology.FieldValue;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The preprocesseer bolt that provides first-round data sanitization
 *  and splits the tweet into four streams of data.
 * 
 * @author Richard Kavanagh
 */
public class TextPreprocessorBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TextPreprocessorBolt.class);
	private static final long serialVersionUID = 5324264730654714029L;
	private static final String NLP_STREAM = "nlp";
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached TextPreprocessor bolt.");
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		
		String id = input.getValueByField(FieldValue.ID.getString()).toString();
		String user = input.getValueByField(FieldValue.USER.getString()).toString();
		String message = preprocessString(input.getValueByField(FieldValue.MESSAGE.getString()).toString());
		String hashtags = preprocessString(input.getValueByField(FieldValue.HASHTAG.getString()).toString());
		
		if (getSentimentAnalysisMode().equals(NLP_STREAM)) {
			LOGGER.info("Taking NLP sentiment analysis stream.");
			collector.emit("nlpSentimentAnalysisStream", new Values(tweet));
		}
		else {
			LOGGER.info("Taking bag-of-words sentiment analysis stream.");
			collector.emit("sentimentAnalysisStream", new Values(message));
		}
		collector.emit("elasticSearchStream", new Values(id, user, message, hashtags));
		collector.emit("entityParseStream", new Values(tweet, message));
	}

	//TODO Get this from configuration in elasticsearch.
	private Object getSentimentAnalysisMode() {
		return "bag-of-words";
	}
	
	private String preprocessString(String input) {
		return input.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("elasticSearchStream", new Fields("tweet_id", "tweet_user", "tweet_message", "tweet_hashtags"));
		declarer.declareStream("entityParseStream", new Fields("tweet", "tweet_message"));
		declarer.declareStream("sentimentAnalysisStream", new Fields("tweet_message"));
		declarer.declareStream("nlpSentimentAnalysisStream", new Fields("tweet"));
	}
}