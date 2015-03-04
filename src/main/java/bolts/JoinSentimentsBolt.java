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

		String id = input.getString(input.fieldIndex("tweet_id"));
		String text = input.getString(input.fieldIndex("tweet_message"));
		int joinedScore = 0;
		boolean postiveJoined = false;
		boolean negativeJoined = false;
		
		if (input.contains("positive_word_score")) {
			int positiveScore = input.fieldIndex("positive_word_score");
			joinedScore += positiveScore;
			if (negativeJoined) {
				collector.emit(new Values(id, text, joinedScore));
			}
			postiveJoined = true;
		}
		else if (input.contains("negative_word_score")){
			int negativeScore = input.fieldIndex("negative_word_score");
			joinedScore += negativeScore;
			if (postiveJoined) {
				collector.emit(new Values(id, text, joinedScore));
			}
			negativeJoined = true;
		}
		else {
			System.out.println("Unknown error occured joining bolts.");
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_sentiment"));
	}
}