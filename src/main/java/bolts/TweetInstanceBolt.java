package bolts;

import java.io.IOException;
import java.util.HashSet;

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
 * The bolt that ensures only one instance of each tweet enters the topology.
 * 
 * @author Richard Kavanagh
 */
public class TweetInstanceBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 42543534L;
	private final static String TEXT_FIELD = "text";

	private ObjectMapper objectMapper = new ObjectMapper();
	private HashSet<String> hashSet = new HashSet<String>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		// jsonData is empty when we get this far due to encoding errors.
		String jsonData = input.getString(0);
		
		JsonNode root;
		try {
			root = objectMapper.readValue(jsonData, JsonNode.class);
			if (hashSet.contains(root.get(TEXT_FIELD))) {
				System.out.println("Tweet already processed.");
				return;
			}
			else {
				collector.emit(new Values(root.toString()));
			}
		}
		catch(JsonParseException err) {
			System.out.println("Malformed JSON entered");
		}
		catch(IOException err) {
			System.out.println("IO error while filtering tweets." + err.getMessage());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}