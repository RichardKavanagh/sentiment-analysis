package bolts;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import twitter4j.Status;
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
	private static final String LANGUAGE = "lan";
	private static final String ENGLISH = "en";
	private static final String HASHTAGS = "hashtagEntities"; 
	
	private ObjectMapper objectMapper = new ObjectMapper();
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		//TODO Status tweet = (Status) input.getValueByField("tweet"); update this bolt.
		
		String jsonData = input.getString(0);
		try {
			JsonNode root = objectMapper.readValue(jsonData, JsonNode.class);
			String user, message, id;

			if (hasValues(root) && isEnglish(root)) {
				id = root.get("id").asText();
				user = root.get("user").asText();
				message = root.get("message").textValue() + getHashTags(root);
				collector.emit(new Values(id, user, message));
			}
		}
		catch(JsonParseException err) {
			System.out.println("Malformed JSON entered, dropping tweet.");
		}
		catch(IOException err) {
			System.out.println("IO error while filtering tweets.");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id", "tweet_message", "tweet_user"));
	}

	private boolean hasValues(JsonNode root) {
		return root.get("user") != null && root.get("message") != null;
	}

	private boolean isEnglish(JsonNode root) {
		 return (root.get(LANGUAGE) != null && ENGLISH.equals(root.get(LANGUAGE).textValue()));
	}

	private String getHashTags(JsonNode root) {
		if (root.get(HASHTAGS) == null) {
			return "";
		}
		else {
			return root.get(HASHTAGS).asText();
		}
	}
}