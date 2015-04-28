package values;

/**
 * Enum containing the possible sentiment results
 *
 * @author Richard Kavanagh.
 */
public enum NLPSentimentValue {

	VERYPOSITIVE("positive"),      
	POSITIVE("somewhat_positive"), 
	VERYNEGATIVE("negative"),  
	NEGATIVE("somewhat_negative"),  
	NEUTRAL("neutral");    

	private String sentiment;

	private NLPSentimentValue(String sentiment) {
		this.sentiment = sentiment;
	}

	public String getSentiment() {
		return sentiment;
	}
}