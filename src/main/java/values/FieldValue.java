package values;

/**
 * Enum containing the various field names in a Twitter status.
 *
 * @author Richard Kavanagh.
 */
public enum FieldValue {

	TWEET("tweet"),
	ID("tweet_id"),
	USER("tweet_user"),
	MESSAGE("tweet_message"),
	HASHTAG("tweet_hashtags"),
	SENTIMENT("tweet_sentiment"),
	SCORE("sentiment_score"),
	POSITIVE("positive_word_score"),
	NEGATIVE("negative_word_score"),
	URL("tweet_URLs"),
	LOCATION("tweet_location");

	private String field;

	private FieldValue(String field) {
		this.field = field;
	}

	public String getString() {
		return field;
	}
}