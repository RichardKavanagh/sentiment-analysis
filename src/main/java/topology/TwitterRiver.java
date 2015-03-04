package topology;

import java.io.IOException;

import org.apache.log4j.Logger;

import spout.TwitterRiverClient;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/*
 * Access point to public twitter streams.
 * 
 * @author Richard Kavanagh.
 */
public class TwitterRiver {

	private static final Logger LOGGER = Logger.getLogger(TwitterRiver.class);
	
	private static ConfigurationBuilder configBuilder;
	private static TwitterRiverClient client;
	
	private static String HOST_NAME = "127.0.0.1";
	private static int PORT = 7777;

	public static void main(String[] args) throws InterruptedException {
		TwitterRiver stream = new TwitterRiver();
		client = new TwitterRiverClient(HOST_NAME, PORT);
		setConfiguration();
		stream.loadMenu();
	}

	public void loadMenu() throws InterruptedException {

		TwitterStream twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {
				try {
					client.writeToTopology(status);
					System.exit(-1);
				} catch (IOException e) {
					LOGGER.info("Error sending status to ThreadPool.");
				}
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				LOGGER.info("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				LOGGER.info("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				LOGGER.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			public void onStallWarning(StallWarning stallWarning) {
			}

			public void onException(Exception err) {
				err.printStackTrace();
			}
		};

		FilterQuery filterQuery = new FilterQuery();
		String keywords [] = { "ireland", "england" };
		String languages [] = { "en" };

		filterQuery.track(keywords);
		filterQuery.language(languages);
		
		twitterStream.addListener(listener);
		twitterStream.filter(filterQuery);
	}

	private static void setConfiguration() {
		configBuilder = new ConfigurationBuilder();
		configBuilder.setDebugEnabled(true);
		configBuilder.setOAuthConsumerKey("n6sg6ModebIgaYGyxK55Pgzvc");
		configBuilder.setOAuthConsumerSecret("s0n1IpItHmZ2tOkTe2SqG1lA1yutkKvcTfcx5VCQk3sMLz4JNh");
		configBuilder.setOAuthAccessToken("2426945665-vOvmXATJyXA1QGMvmaVGTC8MNfiGKbq2RU2xJFt");
		configBuilder.setOAuthAccessTokenSecret("WfLFS2V7BaD1oF8TkVDJ4jTAGVuuShaP6yLN0ngzxnaeP");
	}
}