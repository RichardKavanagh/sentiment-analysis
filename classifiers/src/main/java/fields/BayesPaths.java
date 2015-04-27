
package fields;
/**
 * Enum containing the various file paths used by the BayesClassifier.
 *
 * @author Richard Kavanagh.
 */
public enum BayesPaths {

	SEQUENCE_FILE("src/main/java/input/tweets-seq"),
	LABEL_INDEX("src/main/java/input/labelindex"),
	MODEL("src/main/java/input/model"),
	VECTOR("src/main/java/input/tweets-vectors"),
	DICTIONARY("src/main/java/input/tweets-vectors/dictionary.file-0"),
	DOCUMENT_FREQ("src/main/java/input/tweets-vectors/df-count/part-r-00000");

	private String field;

	private BayesPaths(String field) {
		this.field = field;
	}

	public String getString() {
		return field;
	}
}