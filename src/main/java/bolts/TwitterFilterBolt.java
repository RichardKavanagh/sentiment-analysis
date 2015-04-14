package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The twitter filter bolt that recieves tweets from the logstash spout.
 * 
 * @author Richard Kavanagh
 */
public class TwitterFilterBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
	private static final long serialVersionUID = 7432280938048906081L;

	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Status tweet = (Status) input.getValueByField("tweet");
		if (hasValues(tweet)) {
			long id = tweet.getId();
			String userName = tweet.getUser().getName();
			String message = tweet.getText();
			String hashTags = getHashTags(tweet);
			collector.emit(new Values(tweet, Long.toString(id), userName, message, hashTags));
		}
		else {
			LOGGER.info("Dropping tweet " + tweet.getId());
			LOGGER.info(tweet.getId());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet", "tweet_id", "tweet_user", "tweet_message", "tweet_hashtags"));
	}

	private boolean hasValues(Status tweet) {
		return tweet.getId() != 0
				&& tweet.getUser().getName() != null
				&& tweet.getText() != null ;
	}

	private String getHashTags(Status tweet) {
		if (tweet.getHashtagEntities().length == 0) {
			return "";
		}
		else {
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < tweet.getHashtagEntities().length; i++) {
				stringBuilder.append(tweet.getHashtagEntities()[i].getText()).append(",");
			}
			return stringBuilder.toString();
		}
	}
}