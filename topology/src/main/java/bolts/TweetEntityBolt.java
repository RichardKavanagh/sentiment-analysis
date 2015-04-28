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
 * The parses the media, url and other entities found in the tweet.
 * 
 * @author Richard Kavanagh
 */
public class TweetEntityBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TweetEntityBolt.class);
	private static final long serialVersionUID = 5508385638081026411L;
	
	private String URLs,location,country = "";

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached TweetEntity bolt.");
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		URLs = getURLEntities(tweet);
		location = getLocation(tweet);
		country = getCountry(tweet);
		collector.emit(new Values(URLs, location, country));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.URL.getString(), FieldValue.LOCATION.getString(), FieldValue.COUNTRY.getString()));
	}
	
	private String getLocation(Status tweet) {
		try {
			tweet.getPlace().getName();
		}
		catch(NullPointerException err) {
			LOGGER.info("No location available for tweet");
			return "";
		}
		return tweet.getPlace().getFullName().toString();
	}
	
	private String getCountry(Status tweet) {
		try {
			tweet.getPlace().getCountry();
		}
		catch(NullPointerException err) {
			LOGGER.info("No country available for tweet");
			return "";
		}
		return tweet.getPlace().getCountry().toString();
	}

	private String getURLEntities(Status tweet) {
		if (tweet.getURLEntities().length == 0) {
			LOGGER.info("No url available for tweet");
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