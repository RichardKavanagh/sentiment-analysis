package bolts.NLP;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/*
 * This bolt will perform sentiment analysis usign stanfords openNLP library.
 * 
 * @author Richard Kavanagh.
 */
public class NLPSentimentCalculatorBolt extends BaseBasicBolt {
	

	private static final Logger LOGGER = Logger.getLogger(NLPSentimentCalculatorBolt.class);
	private static final long serialVersionUID = -422962936423672766L;
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		//TODO Implement
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_sentiment"));
	}
}
