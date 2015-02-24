package topology;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/*
 * 
 * @author Richard Kavanagh.
 */
public class TwitterRiver {

	private static ConfigurationBuilder configBuilder;

	public static void main(String[] args) throws InterruptedException {
		TwitterRiver stream = new TwitterRiver();
		setConfiguration();
		stream.loadMenu();
	}

	public void loadMenu() throws InterruptedException {

		TwitterStream twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {
		        //TODO send status to Logstash.
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			public void onStallWarning(StallWarning stallWarning) {
			}

			public void onException(Exception err) {
				err.printStackTrace();
			}
		};
		
		FilterQuery filterQuery = new FilterQuery();
		String keywords[] = {"ireland"};

		filterQuery.track(keywords);
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