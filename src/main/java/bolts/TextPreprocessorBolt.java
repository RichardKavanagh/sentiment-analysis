package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The preprocesseer bolt that provides first-round data sanitization.
 * 
 * @author Richard Kavanagh
 */
public class TextPreprocessorBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TextPreprocessorBolt.class);
	private static final long serialVersionUID = 5324264730654714029L;

	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached TextPreprocessor bolt.");
		String id = input.getValueByField("tweet_id").toString();
		String user = input.getValueByField("tweet_user").toString();
		String message = preprocessString(input.getValueByField("tweet_message").toString());
		String hashtags = preprocessString(input.getValueByField("tweet_hashtags").toString());
		collector.emit("writeMessageStream", new Values(id, message, hashtags));
		message = append(message,hashtags);
		collector.emit("processeMessageStream", new Values(id, user, message));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("processeMessageStream", new Fields("tweet_id", "tweet_user", "tweet_message"));
		declarer.declareStream("writeMessageStream", new Fields("tweet_id", "tweet_message", "tweet_hashtags"));
	}
	

	private String preprocessString(String input) {
		return input.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
	}

	private String append(String message, String hashtags) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(message).append(" [").append(hashtags).append("]");
		return stringBuilder.toString();
	}
}