package topology;

import java.io.IOException;

import org.apache.log4j.Logger;

import spout.TwitterRiverClient;
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
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/*
 * Access point to public twitter streams.
 * 
 * @author Richard Kavanagh.
 */
public class TwitterRiver {

	//TODO change this back to five.
	private static final int MAX_USER_AMOUNT = 1;
	private static final Logger LOGGER = Logger.getLogger(TwitterRiver.class);

	private static ConfigurationBuilder configBuilder;
	private static TwitterRiverClient client;

	private static String HOST_NAME = "127.0.0.1";
	private static int PORT = 7777;

	public static void main(String[] args) throws InterruptedException {
		LOGGER.info("Launching Twitter river.");
		TwitterRiver stream = new TwitterRiver();
		client = new TwitterRiverClient(HOST_NAME, PORT);
		setConfiguration();
		stream.loadMenu();
	}

	public void loadMenu() throws InterruptedException {

		Configuration config = configBuilder.build();
		TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {
				try {
					client.writeToTopology(status);
					/* TODO Remove this when finished testing. */
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
		String keywords [] = getKeyWords();
		String languages [] = getLanguages();
		String [] users = getUsers();

		filterQuery.track(keywords);
		filterQuery.language(languages);

		/*
		if (isFollowingUser()) {
			long [] id = convertUserNameToID(users,config);
			filterQuery.follow(id);
		}
		*/

		twitterStream.addListener(listener);
		twitterStream.filter(filterQuery);
	}

	//TODO Read the following four methods from elasticsearch.
	private String[] getLanguages() {
		String keywords [] = { "obama", "isis" };
		return keywords;
	}

	private String[] getKeyWords() {
		String languages [] = { "en" };
		return languages;
	}

	//TODO When i post on twitter it displays name and what not,but not entering topology.
	private String[] getUsers() {
		String users [] = { "imRichardKav"};
		return users;
	}

	private boolean isFollowingUser() {
		return false;
	}

	/*
	 * Can follow a maximum of five twitter users.
	 */
	private long[] convertUserNameToID(String [] users, Configuration config) {
		long [] userIds = new long [MAX_USER_AMOUNT];
		Twitter twitter = new TwitterFactory(config).getInstance();
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

	private static void setConfiguration() {
		configBuilder = new ConfigurationBuilder();
		configBuilder.setDebugEnabled(true);
		configBuilder.setOAuthConsumerKey("n6sg6ModebIgaYGyxK55Pgzvc");
		configBuilder.setOAuthConsumerSecret("s0n1IpItHmZ2tOkTe2SqG1lA1yutkKvcTfcx5VCQk3sMLz4JNh");
		configBuilder.setOAuthAccessToken("2426945665-vOvmXATJyXA1QGMvmaVGTC8MNfiGKbq2RU2xJFt");
		configBuilder.setOAuthAccessTokenSecret("WfLFS2V7BaD1oF8TkVDJ4jTAGVuuShaP6yLN0ngzxnaeP");
	}
}