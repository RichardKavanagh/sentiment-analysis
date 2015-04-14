package bolts;

import org.apache.log4j.Logger;

import topology.SentimentValue;
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
public class JoinedSentimentCalculator extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
	private static final long serialVersionUID = -4229629366537572766L;
	
	public void execute(Tuple input, BasicOutputCollector collector) {

		int sentimentScore = input.getInteger(input.fieldIndex("tweet_sentiment"));
		
		if (sentimentScore == 0) {
			LOGGER.info("Tweet " + " classified as " + SentimentValue.NEUTRAL.getSentiment());
			collector.emit(new Values(SentimentValue.NEUTRAL.getSentiment()));
		}
		else if (sentimentScore > 0) {
			LOGGER.info("Tweet " + " classified as " + SentimentValue.POSITIVE.getSentiment());
			collector.emit(new Values((SentimentValue.POSITIVE.getSentiment())));
		}
		else {
			LOGGER.info("Tweet " + " classified as " + SentimentValue.NEGATIVE.getSentiment());
			collector.emit(new Values(SentimentValue.NEGATIVE.getSentiment()));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_sentiment"));
	}
}