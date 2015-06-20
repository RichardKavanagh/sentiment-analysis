package bolts;

import java.util.Map;

import org.apache.log4j.Logger;
import utils.ConfigurationSingleton;
import twitter4j.Status;
import values.FieldValue;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The StreamSplitterBolt bolt splits the tweet into four streams of data.
 * 
 * @author Richard Kavanagh
 */
public class StreamSplitterBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(StreamSplitterBolt.class);
	private static final long serialVersionUID = 5324264730654714029L;
	
	private static final String NLP = "nlp";
	private static final String BAG_OF_WORDS = "bag-of-words";
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached StreamSplitter bolt.");
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		
		String id = input.getValueByField(FieldValue.ID.getString()).toString();
		String user = input.getValueByField(FieldValue.USER.getString()).toString();
		String message = input.getValueByField(FieldValue.MESSAGE.getString()).toString();
		String hashtags = input.getValueByField(FieldValue.HASHTAG.getString()).toString();
		String modeOfOperation = getModeOfOperation();
		
		if (modeOfOperation.equals(BAG_OF_WORDS)) {
			LOGGER.info("Taking bag-of-words sentiment analysis stream.");
			collector.emit("sentimentAnalysisStream", new Values(message));
		}
		else if (modeOfOperation.equals(NLP)) {
			LOGGER.info("Taking NLP sentiment analysis stream.");
			collector.emit("nlpSentimentAnalysisStream", new Values(message));
		}
		else {
			LOGGER.info("Taking navie bayes sentiment analysis stream.");
			collector.emit("bayesSentimentAnalysisStream", new Values(message));
			
		}
		collector.emit("elasticSearchStream", new Values(id, user, message, hashtags));
		collector.emit("entityParseStream", new Values(tweet, message));
	}

	private String getModeOfOperation() {
		return ConfigurationSingleton.getInstance().getModeOfOperation();
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("elasticSearchStream", new Fields(FieldValue.ID.getString(),FieldValue.USER.getString(),
				FieldValue.MESSAGE.getString(), FieldValue.HASHTAG.getString()));
		declarer.declareStream("entityParseStream", new Fields(FieldValue.TWEET.getString(), FieldValue.MESSAGE.getString()));
		declarer.declareStream("sentimentAnalysisStream", new Fields(FieldValue.MESSAGE.getString()));
		declarer.declareStream("nlpSentimentAnalysisStream", new Fields(FieldValue.MESSAGE.getString()));
		declarer.declareStream("bayesSentimentAnalysisStream", new Fields(FieldValue.MESSAGE.getString()));
	}
}