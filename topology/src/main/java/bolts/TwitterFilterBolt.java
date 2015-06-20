package bolts;

import org.apache.log4j.Logger;

import twitter4j.Status;
import values.FieldValue;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The twitter filter bolt starts seperating tweet into values.
 * 
 * @author Richard Kavanagh
 */
public class TwitterFilterBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
	private static final long serialVersionUID = 7432280938048906081L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		if (hasValues(tweet)) {
			long id = tweet.getId();
			String userName = tweet.getUser().getName().trim().replace(" ", "_");
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
		declarer.declare(new Fields(FieldValue.TWEET.getString(), FieldValue.ID.getString(), FieldValue.USER.getString(),
				FieldValue.MESSAGE.getString(), FieldValue.HASHTAG.getString()));
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
			return stringBuilder.toString().substring(0, stringBuilder.length()-1);
		}
	}
}