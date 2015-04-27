package elasticsearch;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

import utils.ConfigurationSingleton;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/*
 * Provides access to the topology configuration stored in elasticsearch.
 * 
 * @author Richard Kavanagh.
 */
public class ElasticSearchConfiguration {
	
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchConfiguration.class);
	
	private static String INDEX_NAME = "twitter";
	private static String CONFIG_DOCUMENT = "config";
	
	public void setConfiguration() {
		SearchResponse response = getConfiguration();
		java.util.Iterator<SearchHit> hit_it = response.getHits().iterator();
		JsonNode json = getConfigAsJSON(hit_it);
		JsonNode configInternal = json.get(CONFIG_DOCUMENT);
		ConfigurationSingleton.getInstance().setConfiguration(configInternal);
	}
	
	public SearchResponse getConfiguration() {
		LOGGER.info("Reading ElasticSearch configuration.");
		SearchResponse response = readConfiguration();
		return response;
	}
	
	private JsonNode getConfigAsJSON(java.util.Iterator<SearchHit> hit_it) {
		JsonNode json = null;
		while(hit_it.hasNext()){
			try {
				SearchHit hit = hit_it.next();
				String configJSON = hit.getSourceAsString();
				ObjectMapper mapper = new ObjectMapper();
				json = mapper.readTree(configJSON);
			} catch (JsonProcessingException err) {
				err.printStackTrace();
			} catch (IOException err) {
				err.printStackTrace();
			}
		}
		return json;
	}
	
	private SearchResponse readConfiguration() {
		Client client = getClient();
		SearchResponse response = client.prepareSearch(INDEX_NAME)
		        .setTypes(CONFIG_DOCUMENT)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setFrom(0).setSize(10).setExplain(false)
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