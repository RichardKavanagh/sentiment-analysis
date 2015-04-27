package bolts.nlp;

import org.apache.log4j.Logger;  

import values.FieldValue;
import values.NLPSentimentValue;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * This bolt will calculate the sentiment passed to it by the NLPSentiment bolt.
 * 
 * @author Richard Kavanagh.
 */
public class NLPSentimentCalculatorBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(NLPSentimentCalculatorBolt.class);
	private static final long serialVersionUID = -42272936423672766L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached NLPSentimentCalculatorBolt.");

		int sentimentScore = input.getInteger(input.fieldIndex(FieldValue.SCORE.getString()));
		switch (sentimentScore) {
		case 0:
			LOGGER.info("Tweet " + " classified as " + NLPSentimentValue.VERYNEGATIVE.getSentiment());
			collector.emit(new Values(NLPSentimentValue.VERYNEGATIVE.getSentiment(), sentimentScore));
			break;
		case 1:
			LOGGER.info("Tweet " + " classified as " + NLPSentimentValue.NEGATIVE.getSentiment());
			collector.emit(new Values(NLPSentimentValue.NEGATIVE.getSentiment(), sentimentScore));
			break;
		case 2:
			LOGGER.info("Tweet " + " classified as " + NLPSentimentValue.NEUTRAL.getSentiment());
			collector.emit(new Values(NLPSentimentValue.NEUTRAL.getSentiment(), sentimentScore));
			break;
		case 3:
			LOGGER.info("Tweet " + " classified as " + NLPSentimentValue.POSITIVE.getSentiment());
			collector.emit(new Values(NLPSentimentValue.POSITIVE.getSentiment(), sentimentScore));
			break;
		case 4:
			LOGGER.info("Tweet " + " classified as " + NLPSentimentValue.VERYPOSITIVE.getSentiment());
			collector.emit(new Values(NLPSentimentValue.VERYPOSITIVE.getSentiment(), sentimentScore));
			break;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.SENTIMENT.getString(), FieldValue.SCORE.getString()));
	}
}
