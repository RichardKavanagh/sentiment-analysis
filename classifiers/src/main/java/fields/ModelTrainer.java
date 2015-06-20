package fields;

/**
 * Enum containing the various argument values for the bayes model training code.
 *
 * @author Richard Kavanagh.
 */
public enum ModelTrainer {

	TRAIN_COMPLEMENTARY("-c"),
	LABEL_INDEX("-li"),
	EXTRACT_LABELS("-el"),
	OVERWRITE_OUTPUT("-ow"),
	OUTPUT_DIR("-o"),
	INPUT_DIR("-i");

	private String field;

	private ModelTrainer(String field) {
		this.field = field;
	}

	public String getString() {
		return field;
	}
}