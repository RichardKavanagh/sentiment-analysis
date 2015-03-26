package bolts;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/*
 * The bolt that stores results in ElasticSearch.
 * 
 * @author Richard Kavanagh
 */
public class ElasticSearchWriterBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(ElasticSearchWriterBolt.class);
	private static final long serialVersionUID = -4229629366537572766L;
	
	private static String INDEX_NAME = "twitter";
	private static String DOCUMENT_TYPE = "tweet";
	
	private Client elasticSearchClient;
	private String documentId = "1";

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		/*	TODO Account for taking in from two different bolts. */

		LOGGER.info("In ElasticSearch writer bolt.");
		
		elasticSearchClient = getClient();
		
        final IndicesExistsResponse result = elasticSearchClient.admin().indices().prepareExists(INDEX_NAME).execute().actionGet();
        if (!result.isExists()) {
        	createIndex();
        }
        final IndexRequestBuilder indexRequestBuilder = elasticSearchClient.prepareIndex(INDEX_NAME, DOCUMENT_TYPE, documentId);
        
        /* Build JSON-Mapping object. */
        XContentBuilder contentBuilder = buildJSON(indexRequestBuilder);
        indexRequestBuilder.setSource(contentBuilder);
        indexRequestBuilder.execute().actionGet();
	}

	private void createIndex() {
		final CreateIndexRequestBuilder createIndexRequestBuilder = elasticSearchClient.admin().indices().prepareCreate(INDEX_NAME);
		createIndexRequestBuilder.addMapping(DOCUMENT_TYPE, getMapping());
		createIndexRequestBuilder.execute().actionGet();
	}

	private XContentBuilder buildJSON(final IndexRequestBuilder indexRequestBuilder) {
		XContentBuilder contentBuilder = null;
		try {
			contentBuilder = jsonBuilder().startObject().startObject(DOCUMENT_TYPE);
			contentBuilder.field("user", "richard");
			contentBuilder.field("message", "I am making tweets");
			contentBuilder.endObject().endObject();
		} catch (IOException err) {
			err.printStackTrace();
		}
		return contentBuilder;
	}
	
	private Client getClient() {
        final ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return transportClient;
    }
	
	private XContentBuilder getMapping() {
		XContentBuilder mappingBuilder = null;
		try {
			mappingBuilder = jsonBuilder().startObject().startObject(DOCUMENT_TYPE)
	                .startObject("_ttl").field("enabled", "true").field("default", "1s").endObject().endObject()
	                .endObject();
		} catch (IOException err) {
			err.printStackTrace();
		}
		return mappingBuilder;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
