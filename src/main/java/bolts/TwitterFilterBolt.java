package bolts;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * The twitter filter bolt that recieves tweets from the logstash spout.
 * 
 * @author Richard Kavanagh
 */
public class TwitterFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 7432280938048906081L;
	
	private ObjectMapper objectMapper = new ObjectMapper();

	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	//TODO We need some global tweet_id variable to track tweets.
	public void execute(Tuple input, BasicOutputCollector collector) {
		String jsonData = input.getString(0);
		try {
			JsonNode root = objectMapper.readValue(jsonData, JsonNode.class);
			String user, message;

			if (hasValues(root) && isEnglish(root)) {
				user = root.get("user").asText();
				message = root.get("message").textValue() + getHashTags();
				collector.emit(new Values(user, message));
			}
		}
		catch(JsonParseException err) {
			System.out.println("Malformed JSON entered");
		}
		catch(IOException err) {
			System.out.println("IO error while filtering tweets");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_message", "tweet_user"));
	}

	private boolean hasValues(JsonNode root) {
		return root.get("user") != null && root.get("message") != null;
	}

	/*
	 * Determines if a tweet is primarily english.
	 */
	private boolean isEnglish(JsonNode root) {
		return true;
		//TODO Twitter4J should have functionality to do this.
	}

	/*
	 * As hash tags are returned as part of the message we need to parse them.
	 */
	private String getHashTags() {
		return " #hashtag";
	}
}