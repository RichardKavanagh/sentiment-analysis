package bolts;

import org.apache.log4j.Logger;

import topology.FieldValue;
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

		if (input.contains(FieldValue.POSITIVE.getString())) {
			int positiveScore = input.getInteger((input.fieldIndex(FieldValue.POSITIVE.getString())));
			joinedScore += positiveScore;
			postiveJoined = true;
			if (negativeJoined) {
				collector.emit(new Values(joinedScore));
				resetFlags();
			}
		}
		else if (input.contains(FieldValue.NEGATIVE.getString())) {
			int negativeScore = input.getInteger(input.fieldIndex(FieldValue.NEGATIVE.getString()));
			joinedScore -= negativeScore;
			negativeJoined = true;
			if (postiveJoined) {
				collector.emit(new Values(joinedScore));
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
		declarer.declare(new Fields("tweet_sentiment"));
	}
}