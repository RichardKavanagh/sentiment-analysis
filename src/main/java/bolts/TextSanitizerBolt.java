package bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/*
 * The TextSanitizerBolt bolt that provides further sanitization by stripping away certain words.
 * 
 * @author Richard Kavanagh
 */
public class TextSanitizerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4349364405881264772L;
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		/*
		 * Purpose:
		 * 	Removal of stemming/stop words/suffixes that are not useful.
		 */
	}   

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_timestamp", "tweet_message", "tweet_user", "tweet_hashtags"));
	}
}