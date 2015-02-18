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
 * The preprocesseer bolt that provides first-round data sanitization.
 * 
 * @author Richard Kavanagh
 */
public class TextPreprocessorBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 5324264730654714029L;
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		System.out.println(input.toString());
		/*
		 * Purpose:
		 * 	Remove non-alpha characters and unsupported encoding from tweet text
		 */
	}   

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_message", "tweet_user", "tweet_hashtags"));
	}
}