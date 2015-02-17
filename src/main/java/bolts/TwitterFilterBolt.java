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
 * The filter bolt that recieves tweets from the logstash spout.
 * 
 * @author Richard Kavanagh
 */
public class TwitterFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 7432280938048906081L;
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		/*
		 * Purpose:
		 *  Parse incoming JSON and get timestamp, user and text.
		 *  Disregard non-english tweets.
		 *  Gather hashtags if any.
		 */
	}   

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_timestamp", "tweet_message", "tweet_user", "tweet_hashtags"));
	}
}