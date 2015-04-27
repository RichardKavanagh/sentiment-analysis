package bolts.nlp;

import nlp.NLPSentimentAnalyzer;

import org.apache.log4j.Logger;

import values.FieldValue;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * This bolt will perform sentiment analysis usign stanfords openNLP library.
 * 
 * @author Richard Kavanagh.
 */
public class NLPSentimentBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(NLPSentimentBolt.class);
	private static final long serialVersionUID = -422962936423672766L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached NLPSentimentBolt.");
		String status = input.getString(input.fieldIndex(FieldValue.MESSAGE.getString()));
		NLPSentimentAnalyzer classifier = new NLPSentimentAnalyzer();
		int sentiment = 0;
		sentiment = classifier.calculateSentiment(status, sentiment);
		collector.emit(new Values(sentiment));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.SCORE.getString()));
	}
}