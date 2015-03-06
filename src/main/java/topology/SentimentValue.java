package topology;

/**
 * Enum containing the bag-of-words sentiment results
 *
 * @author Richard Kavanagh.
 */
public enum SentimentValue {

	POSITIVE("positive"),
	NEGATIVE("negative"),
	NEUTRAL("neutral");

	private String sentiment;

	private SentimentValue(String sentiment) {
		this.sentiment = sentiment;
	}

	public String getSentiment() {
		return sentiment;
	}
}