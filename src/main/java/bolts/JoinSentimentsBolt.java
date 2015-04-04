package bolts;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Joins the results from the positive/negative bolts.
 * 
 * @author Richard Kavanagh.
 */
public class JoinSentimentsBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(JoinSentimentsBolt.class);
	private static final long serialVersionUID = 3640971489699420669L;
	
	private boolean postiveJoined = false;
	private boolean negativeJoined = false;
	private int joinedScore = 0;

	public void execute(Tuple input, BasicOutputCollector collector) {

		String id = input.getString(input.fieldIndex("tweet_id"));
		String text = input.getString(input.fieldIndex("tweet_message"));
		
		if (input.contains("positive_word_score")) {
			int positiveScore = input.getInteger((input.fieldIndex("positive_word_score")));
			joinedScore += positiveScore;
			postiveJoined = true;
			if (negativeJoined) {
				collector.emit(new Values(id, text, joinedScore));
				resetFlags();
			}
		}
		else if (input.contains("negative_word_score")) {
			int negativeScore = input.getInteger(input.fieldIndex("negative_word_score"));
			joinedScore -= negativeScore;
			negativeJoined = true;
			if (postiveJoined) {
				collector.emit(new Values(id, text, joinedScore));
				resetFlags();
			}
		}
		else {
			LOGGER.info("Unknown error occured joining bolts.");
		}
	}

	private void resetFlags() {
		postiveJoined = false;
		negativeJoined = false;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_sentiment"));
	}
}