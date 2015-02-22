package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/*
 * The bolt that calaulates the amount of positive words in a String.
 * 
 * @author Richard Kavanagh
 */
public class PositiveWordsBolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("tweet_id", "positive_word_score", "tweet_message"));
	}

	
}

