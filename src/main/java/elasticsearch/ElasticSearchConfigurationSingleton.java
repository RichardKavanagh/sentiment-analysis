package elasticsearch;

/*
 * @author Richard Kavanagh.
 */
public class ElasticSearchConfigurationSingleton {
	
	private static ElasticSearchConfigurationSingleton instance = null;
	
	private boolean available = false;
	
	/*
	 * Exists only to defeat instantiation.
	 */
	protected ElasticSearchConfigurationSingleton() {
	}
	
	/*
	 * Synchronized to avoid race conditions.
	 */
	public static synchronized ElasticSearchConfigurationSingleton getInstance() {
		if(instance == null) {
			instance = new ElasticSearchConfigurationSingleton();
		}
		return instance;
	}
	
	/* One main method to read in the configuration. */
	public void setConfiguration() {
		
	}
	
	/* Then mulitple getters to read the five types.*/
}
