package topology;

import java.util.Map;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * 
 * @author Richard Kavanagh
 */
public class TweetPreprocessorBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 5324264730654714029L;
	private ObjectMapper objectMapper = new ObjectMapper();

	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		Status tweet = (Status) input.getValueByField("tweet");
		
		/*
		 * We can now call tweet.getUser() etc to get field values.
		 * 
		 */
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}
}