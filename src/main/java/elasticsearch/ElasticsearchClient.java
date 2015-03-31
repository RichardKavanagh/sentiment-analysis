package elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import backtype.storm.tuple.Tuple;


/*
 * Elasticsearch clint class.
 * 
 * @author Richard Kavanagh.
 */
public class ElasticsearchClient {

	private static final Logger LOGGER = Logger.getLogger(ElasticsearchClient.class);
	private static String INDEX_NAME = "twitter";
	private static String DOCUMENT_TYPE = "tweet";

	private IndexRequestBuilder indexRequestBuilder;
	private Client elasticSearchClient;
	private int documentId;

	private String host;
	private int port;

	public ElasticsearchClient() {
		elasticSearchClient = getClient();
		init();
	}

	/*
	 * Checks if the index being write to exists , and if not creates it.
	 */
	public void init() {
		documentId = 2;
		if (elasticSearchClient == null) {
			elasticSearchClient = getClient();
		}
		final IndicesExistsResponse result = elasticSearchClient.admin().indices().prepareExists(INDEX_NAME).execute().actionGet();
		if (!result.isExists()) {
			createIndex();
		}
		indexRequestBuilder = elasticSearchClient.prepareIndex(INDEX_NAME, DOCUMENT_TYPE, Integer.toString(documentId));
	}

	public void write() {
		documentId++;
		XContentBuilder contentBuilder = buildJSON(indexRequestBuilder);
		indexRequestBuilder.setSource(contentBuilder);
		IndexResponse indexResponse = indexRequestBuilder.execute().actionGet();
		LOGGER.info("Wrote to elasticsearch " + indexResponse.toString());
	}

	public Client getClient() {
		final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
		TransportClient transportClient = new TransportClient(settings);
		transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		return transportClient;
	}

	private XContentBuilder buildJSON(final IndexRequestBuilder indexRequestBuilder) {
		XContentBuilder contentBuilder = null;
		try {
			contentBuilder = jsonBuilder().startObject().startObject(DOCUMENT_TYPE);
			contentBuilder.field("user", "richard");
			contentBuilder.field("message", "I am making tweets");
			contentBuilder.field("sentiment", "positive");
			contentBuilder.field("timestamp", "1243232143");
			contentBuilder.endObject().endObject();
		} catch (IOException err) {
			err.printStackTrace();
		}
		return contentBuilder;
	}

	private void createIndex() {
		LOGGER.info("Creating elasticsearch index " + INDEX_NAME);
		final CreateIndexRequestBuilder createIndexRequestBuilder = elasticSearchClient.admin().indices().prepareCreate(INDEX_NAME);
		createIndexRequestBuilder.addMapping(DOCUMENT_TYPE, getMapping());
		createIndexRequestBuilder.execute().actionGet();
	}

	/*
	 * Generates mapping for index. 
	 */
	private XContentBuilder getMapping() {
		XContentBuilder mappingBuilder = null;
		try {
			mappingBuilder = XContentFactory.jsonBuilder().startObject()  
				.startObject(DOCUMENT_TYPE)
					.field("user", "string")
					.field("message", "string")
					.field("sentiment","string")
				.endObject()
				.startObject("timestamp")
					.field("type", "long")
					.field("store", "yes")
					.field("index","not_analyzed")
				.endObject();
		} catch (IOException err) {
			err.printStackTrace();
		}
		return mappingBuilder;
	}

	/*
	 * Basic getters and setters. 
	 */
	public int getPort() {
		return this.port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}
}