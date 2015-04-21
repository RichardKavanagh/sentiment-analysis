package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import clojure.lang.IFn.L;
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
 * The TextSanitizerBolt bolt that provides further sanitization by stripping away stemming/stop words/suffixes.
 * 
 * @author Richard Kavanagh
 */
public class TextPreProcessorBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TextPreProcessorBolt.class);
	private static final long serialVersionUID = -8171045339897756375L;
	
	private static final String HTTP_DELIMITER = "http";
	private static final String RETWEET_DELIMITER = "RT";
	private static final CharSequence NULL_TEXT = "null";
	private static final String SPACE = " ";
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached TextPreProcessorBolt bolt.");
		
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		String id = input.getValueByField(FieldValue.ID.getString()).toString();
		String user = input.getValueByField(FieldValue.USER.getString()).toString();
		String hashtags = input.getValueByField(FieldValue.HASHTAG.getString()).toString();
		
		String message = input.getValueByField(FieldValue.MESSAGE.getString()).toString();
		message = preprocessString(message);
		
		if (!message.contains(NULL_TEXT)) {
			collector.emit(new Values(tweet, id, user, message, hashtags));
		}
		else {
			LOGGER.info("Dropping Tweet: Id " + id);
		}
	}
	
	private String preprocessString(String input) {
		String processedInput = input;
		processedInput = removeRetweetChars(processedInput);
		processedInput = removeNoise(processedInput);
		processedInput = removeURLs(processedInput);
		return processedInput;
	}

	private String removeURLs(String processedInput) {
		String [] tokens = processedInput.split(SPACE);
		StringBuilder stringBuilder = new StringBuilder();
		for (String token : tokens) {
			if (token.startsWith(HTTP_DELIMITER) == false) {
				stringBuilder.append(token).append(SPACE);
			}
		}
		return stringBuilder.toString();
	}

	private String removeNoise(String processedInput) {
		return processedInput.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
	}

	private String removeRetweetChars(String processedInput) {
		if (processedInput.contains(RETWEET_DELIMITER)) {
			processedInput = processedInput.substring(2);
		}
		return processedInput;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.TWEET.getString(), FieldValue.ID.getString(), FieldValue.USER.getString(),
				FieldValue.MESSAGE.getString(), FieldValue.HASHTAG.getString()));
	}
}