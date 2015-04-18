package topology;

import java.io.IOException;

import org.apache.log4j.Logger;


/*
 * Access point to public twitter streams.
 * 
 * @author Richard Kavanagh.
 */


import java.util.ArrayList;
import java.util.List;

import spout.TwitterRiverClient;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class TwitterRiver {
	
	private static TwitterRiverClient client;
	private static String HOST_NAME = "127.0.0.1";
	private static int PORT = 7777;

	public static void main(String [] agrs) throws IOException {

		String topic = "gamergate";

    	client = new TwitterRiverClient(HOST_NAME, PORT);
        Twitter twitter = new TwitterFactory().getInstance();
        ArrayList<String> tweetList = new ArrayList<String>();
        try {
            Query query = new Query(topic);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                	client.writeToTopology(tweet);
                	System.exit(-1);
                }
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
        }
    }
}


