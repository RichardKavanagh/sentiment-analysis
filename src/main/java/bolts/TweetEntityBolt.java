package bolts;

import org.apache.log4j.Logger;

import twitter4j.Status;
import values.FieldValue;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The parses the media, url and other entities found in the tweet.
 * 
 * @author Richard Kavanagh
 */
public class TweetEntityBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TweetEntityBolt.class);
	private static final long serialVersionUID = 5508385638081026411L;
	
	private String URLs,location = "";

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached TweetEntity bolt.");
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		URLs = getURLEntities(tweet);
		location = getLocation(tweet);
		collector.emit(new Values(URLs, location));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.URL.getString(), FieldValue.LOCATION.getString()));
	}
	
	private String getLocation(Status tweet) {
		if (tweet.getGeoLocation() == null) {
			return "";
		}
		else {
			return tweet.getGeoLocation().toString();
		}
	}

	private String getURLEntities(Status tweet) {
		if (tweet.getURLEntities().length == 0) {
			return "";
		}
		else {
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < tweet.getURLEntities().length; i++) {
				stringBuilder.append(tweet.getURLEntities()[i].getText()).append(",");
			}
			return stringBuilder.toString().substring(0, stringBuilder.toString().length() -1);
		}
	}
}