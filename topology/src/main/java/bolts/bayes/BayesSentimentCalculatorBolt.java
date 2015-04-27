package bolts.bayes;

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
 * This bolt will calcuate the sentiment based on the score provided by the BayesSentimentBolt.
 * 
 * @author Richard Kavanagh.
 */
public class BayesSentimentCalculatorBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(BayesSentimentCalculatorBolt.class);
	private static final long serialVersionUID = 422962125323672766L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached bayes sentiment calculator bolt.");
		int sentimentScore = input.getInteger(input.fieldIndex(FieldValue.SCORE.getString()));
		
		switch (sentimentScore) {
		case 1:
			LOGGER.info("Tweet " + " classified as " + SentimentValue.POSITIVE.getSentiment());
			collector.emit(new Values(SentimentValue.POSITIVE.getSentiment(), sentimentScore));
			break;
		case 0:
			LOGGER.info("Tweet " + " classified as " + SentimentValue.NEGATIVE.getSentiment());
			collector.emit(new Values(SentimentValue.NEGATIVE.getSentiment(), sentimentScore));
			break;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.SENTIMENT.getString(), FieldValue.SCORE.getString()));
	}
}