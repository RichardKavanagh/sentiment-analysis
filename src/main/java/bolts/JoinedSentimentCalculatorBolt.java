package bolts;

import org.apache.log4j.Logger;

import values.FieldValue;
import values.SentimentValue;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Takes the joined positive/negative bolt scores and calculates final sentiment value.
 * 
 * @author Richard Kavanagh
 */
public class JoinedSentimentCalculatorBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
	private static final long serialVersionUID = -4229629366537572766L;
	
	public void execute(Tuple input, BasicOutputCollector collector) {

		int sentimentScore = input.getInteger(input.fieldIndex(FieldValue.SENTIMENT.getString()));
		
		if (sentimentScore == 0) {
			LOGGER.info("Tweet " + " classified as " + SentimentValue.NEUTRAL.getSentiment());
			collector.emit(new Values(SentimentValue.NEUTRAL.getSentiment(), sentimentScore));
		}
		else if (sentimentScore > 0) {
			LOGGER.info("Tweet " + " classified as " + SentimentValue.POSITIVE.getSentiment());
			collector.emit(new Values(SentimentValue.POSITIVE.getSentiment(), sentimentScore));
		}
		else {
			LOGGER.info("Tweet " + " classified as " + SentimentValue.NEGATIVE.getSentiment());
			collector.emit(new Values(SentimentValue.NEGATIVE.getSentiment(), sentimentScore));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.SENTIMENT.getString(), FieldValue.SCORE.getString()));
	}
}