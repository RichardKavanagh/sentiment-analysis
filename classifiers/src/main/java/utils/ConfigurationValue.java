package utils;

/**
 * Enum containing the various field names used for topology configuration.
 *
 * @author Richard Kavanagh.
 */
public enum ConfigurationValue {

	LIMIT("limit"),
	RETWEETS("retweets"),
	LANGUAGE("language"),
	SYSTEM_STATE("system_active"),
	OPERATION_MODE("mode_of_operation"),
	KEYWORDS("keywords"),
	USERS("users");

	private String field;

	private ConfigurationValue(String field) {
		this.field = field;
	}

	public String getString() {
		return field;
	}
}