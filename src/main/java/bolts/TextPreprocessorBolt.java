package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

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
 *  and splits the tweet into three streams of data.
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
		Status tweet = (Status) input.getValueByField("tweet");
		
		String id = input.getValueByField("tweet_id").toString();
		String user = input.getValueByField("tweet_user").toString();
		String message = preprocessString(input.getValueByField("tweet_message").toString());
		String hashtags = preprocessString(input.getValueByField("tweet_hashtags").toString());
		
		collector.emit("sentimentAnalysisStream", new Values(message));
		collector.emit("elasticSearchStream", new Values(id, user, message, hashtags));
		collector.emit("entityParseStream", new Values(tweet, message));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("elasticSearchStream", new Fields("tweet_id", "tweet_user", "tweet_message", "tweet_hashtags"));
		declarer.declareStream("entityParseStream", new Fields("tweet", "tweet_message"));
		declarer.declareStream("sentimentAnalysisStream", new Fields("tweet_message"));
	}
	

	private String preprocessString(String input) {
		return input.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
	}
}