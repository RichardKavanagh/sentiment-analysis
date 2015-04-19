package values;

/**
 * Enum containing the possible sentiment results for the bag-of-words model.
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