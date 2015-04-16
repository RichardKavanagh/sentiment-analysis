package elasticsearch;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/*
 * Provides access to the topology configuration stored in elasticsearch.
 * 
 * @author Richard Kavanagh.
 */
public class ElasticSearchConfiguration {
	
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchConfiguration.class);
	
	private static String INDEX_NAME = "twitter";
	private static String CONFIG_DOCUMENT = "config";
	
	public String getConfiguration() {
		LOGGER.info("Reading ElasticSEarch configuration.");
		SearchResponse response = readConfiguration();
		String config = response.getHits().getHits().toString();
		return config;
	}
	
	private SearchResponse readConfiguration() {
		Client client = getClient();
		SearchResponse response = client.prepareSearch(INDEX_NAME)
		        .setTypes(CONFIG_DOCUMENT)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setFrom(0).setSize(1).setExplain(true)
		        .execute()
		        .actionGet();
		return response;
	}
	
	public Client getClient() {
		final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
		TransportClient transportClient = new TransportClient(settings);
		transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		return transportClient;
	}
}
