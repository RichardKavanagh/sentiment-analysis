package bolts.bayes;

import java.util.Map;

import org.apache.log4j.Logger;

import values.FieldValue;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bayes.NaiveBayesClassifier;

/*
 * This bolt will perform sentiment analysis usign a naive bayes classifier.
 * 
 * @author Richard Kavanagh.
 */
public class BayesSentimentBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(BayesSentimentBolt.class);
	private static final long serialVersionUID = 422962945323672766L;

	private NaiveBayesClassifier bayesClassifier;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		bayesClassifier = new NaiveBayesClassifier();
	}
	

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached bayes sentiment bolt.");
		String status = input.getString(input.fieldIndex(FieldValue.MESSAGE.getString()));
		int sentiment = bayesClassifier.classifyText(status);
		collector.emit(new Values(sentiment));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.SCORE.getString()));
	}
}