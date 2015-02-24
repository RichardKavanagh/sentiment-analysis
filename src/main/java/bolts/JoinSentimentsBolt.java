package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinSentimentsBolt extends BaseBasicBolt {
	
	
	private static final long serialVersionUID = 3640971489699420669L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		/*
		 * Will join the scores of PositiveWordsBolt and NegativeWordsBolt
		 * to calculate tweet sentiment score.
		 * 
		 */
		
	}
	
	private void emit(BasicOutputCollector collector, Long id, String text, float positiveScore, float negativeScore) {
		collector.emit(new Values(id, positiveScore, negativeScore, text));
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("tweet_id", "positive_score", "negative_score","tweet_text"));
	}
}