package elasticsearch;

import values.ConfigurationValue;

import com.fasterxml.jackson.databind.JsonNode;

/*
 * Holds the configuration settings stored in ElasticSearch,
 * and acts as an access point to this data for the topology.
 * 
 * @author Richard Kavanagh.
 */
public class ConfigurationSingleton {
	
	private static ConfigurationSingleton instance = null;
	
	private int limit = 0;
	private boolean retweets = false;
	private boolean systemState = false;
	private String language = "";
	private String operationMode = "";
	private String [] keywords = new String [5];
	private String [] users = new String [5];
	
	/*
	 * Exists only to defeat instantiation.
	 */
	protected ConfigurationSingleton() {
	}
	
	/*
	 * Synchronized to avoid race conditions.
	 */
	public static synchronized ConfigurationSingleton getInstance() {
		if(instance == null) {
			instance = new ConfigurationSingleton();
		}
		return instance;
	}
	
	/* 
	 * One main method to read in the configuration.
	 */
	public synchronized void setConfiguration(JsonNode configInternal) {
		limit = configInternal.get(ConfigurationValue.LIMIT.getString()).asInt();
		retweets = configInternal.get(ConfigurationValue.RETWEETS.getString()).asBoolean();
		language = configInternal.get(ConfigurationValue.LANGUAGE.getString()).textValue();
		systemState = configInternal.get(ConfigurationValue.SYSTEM_STATE.getString()).asBoolean();
		operationMode = configInternal.get(ConfigurationValue.OPERATION_MODE.getString()).textValue();
		
		JsonNode keywordsNode = configInternal.get(ConfigurationValue.KEYWORDS.getString());
		setKeywords(keywordsNode);
		
		JsonNode usersNode = configInternal.get(ConfigurationValue.USERS.getString());
		setUsers(keywordsNode, usersNode);
	}

	private void setUsers(JsonNode keywordsNode, JsonNode usersNode) {
		if (keywordsNode.isArray()) {
			int j = 0;
		    for (final JsonNode objectNode : usersNode) {
		        users[j] = objectNode.textValue();
		        j++;
		    }
		}
	}

	private void setKeywords(JsonNode keywordsNode) {
		if (keywordsNode.isArray()) {
			int i = 0;
		    for (final JsonNode objectNode : keywordsNode) {
		        keywords[i] = objectNode.textValue();
		        i++;
		    }
		}
	}

	/*
	 * Collection of getter's to be used throughout topology.
	 */
	public  int getLimit() {
		return limit;
	}
	
	public  boolean getRetweets() {
		return retweets;
	}
	
	public  String getLanguage() {
		return language;
	}
	
	public  boolean getSystemActive() {
		return systemState;
	}
	
	public  String getModeOfOperation() {
		return operationMode;
	}
	
	public  String [] getKeywords() {
		return keywords;
	}
	
	public  String [] getUsers() {
		return users;
	}
}