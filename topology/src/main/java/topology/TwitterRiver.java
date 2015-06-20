package topology;

import java.io.IOException;

import org.apache.log4j.Logger;

import utils.ConfigurationSingleton;
import elasticsearch.ElasticSearchConfiguration;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import utils.TwitterRiverClient;

/*
 * Access point to public twitter streams.
 * 
 * @author Richard Kavanagh.
 */
public class TwitterRiver {

	private static final Logger LOGGER = Logger.getLogger(TwitterRiver.class);
	private static final int MAX_USER_AMOUNT = 5;
	private static TwitterRiverClient client;
	private static int PORT = 7777;
	private static String HOST_NAME = "127.0.0.1";
	
	private ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();

	public static void main(String[] args) throws InterruptedException {
		LOGGER.info("Launching Twitter river.");
		TwitterRiver stream = new TwitterRiver();
		client = new TwitterRiverClient(HOST_NAME, PORT);
		stream.loadMenu();
	}

	public void loadMenu() throws InterruptedException {
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		StatusListener listener = new StatusListener() {
			public void onStatus(Status status) {
				esConfig.setConfiguration();
				try {
					client.writeToTopology(status);
					try {
						Thread.sleep(3000);
					} catch (InterruptedException err) {
						Thread.currentThread().interrupt();
					}
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
				LOGGER.info("Got stall warning:" + stallWarning.getMessage());
			}
			
			public void onException(Exception err) {
				LOGGER.info("Exception occurred:" + err.getMessage() + err.getStackTrace());
			}
		};
		
		esConfig.setConfiguration();
		setTwitterStreamValues(twitterStream, listener);
	}

	private void setTwitterStreamValues(TwitterStream twitterStream, StatusListener listener) {
		FilterQuery filterQuery = new FilterQuery();

		String keywords [] = ConfigurationSingleton.getInstance().getKeywords();
		String [] languages = { ConfigurationSingleton.getInstance().getLanguage() };
		String [] users = ConfigurationSingleton.getInstance().getUsers();
		long [] id = convertUserNameToID(users);
		
		filterQuery.follow(id);
		filterQuery.track(keywords);
		filterQuery.language(languages);
		
		twitterStream.addListener(listener);
		twitterStream.filter(filterQuery);
	}

	/*
	 * Can follow a maximum of five twitter users.
	 */
	private long[] convertUserNameToID(String [] users) {
		long [] userIds = new long [MAX_USER_AMOUNT];
		Twitter twitter = new TwitterFactory().getInstance();
		for (int i = 0; i != 1; i++) {
			User user;
			try {
				user = twitter.showUser(users[i]);
				if (user != null) {
					long userId = user.getId();
					userIds[i] = userId;
				}
			} catch (TwitterException err) {
				LOGGER.info("Swallow user not found exception.");
			}
		}
		return userIds;
	}
}