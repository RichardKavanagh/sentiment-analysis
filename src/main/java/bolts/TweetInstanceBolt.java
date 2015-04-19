package bolts;

import java.util.HashSet;

import org.apache.log4j.Logger;

import elasticsearch.ConfigurationSingleton;
import twitter4j.Status;
import values.FieldValue;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The bolt that ensures only one instance of each tweet enters the topology.
 * 
 * @author Richard Kavanagh
 */
public class TweetInstanceBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TweetInstanceBolt.class);
	private static final long serialVersionUID = 425435354634L;

	private HashSet<String> hashSet = new HashSet<String>();

	public synchronized void execute(Tuple input, BasicOutputCollector collector) {
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		if (hashSet.contains(tweet.getText())) {
			LOGGER.info("Tweet already processed.");
			return;
		}
		else if(ConfigurationSingleton.getInstance().getRetweets() == false && tweet.isRetweet()) {
			LOGGER.info("Tweet is a retweet, dropping from topology.");
			return;
		}
		else {
			hashSet.add(tweet.getText());
			LOGGER.info("Adding tweet to topology. " + tweet.getId());
			collector.emit(new Values(tweet));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.TWEET.getString()));
	}
}