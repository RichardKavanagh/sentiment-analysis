package values;

/**
 * Enum containing the possible sentiment results
 *
 * @author Richard Kavanagh.
 */
public enum NLPSentimentValue {

	VERYPOSITIVE("positive"),      
	POSITIVE("somewhat positive"), 
	VERYNEGATIVE("negative"),  
	NEGATIVE("somewhat negative"),  
	NEUTRAL("neutral");    

	private String sentiment;

	private NLPSentimentValue(String sentiment) {
		this.sentiment = sentiment;
	}

	public String getSentiment() {
		return sentiment;
	}
}