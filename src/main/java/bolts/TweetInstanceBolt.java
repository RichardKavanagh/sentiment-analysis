package bolts;

import java.util.HashSet;

import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The bolt that ensures only one instance of each tweet enters the topology.
 * 
 * @author Richard Kavanagh
 */
public class TweetInstanceBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 42543534L;

	private HashSet<String> hashSet = new HashSet<String>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		Status tweet = (Status) input.getValueByField("tweet");
		if (hashSet.contains(tweet.toString())) {
			System.out.println("Tweet already processed.");
			return;
		}
		else {
			hashSet.add(tweet.toString());
			System.out.println("Adding tweet to topology. " + tweet.getId());
			collector.emit(new Values(tweet));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}