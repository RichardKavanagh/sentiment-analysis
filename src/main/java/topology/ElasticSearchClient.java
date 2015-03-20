package topology;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;

import org.elasticsearch.common.settings.ImmutableSettings;

/*
 * @author Richard Kavanagh.
 */
public class ElasticSearchClient {


	private String ElasticsearchAddress = "http://localhost:9200";
	private String twitterIndex = "twitter";
	private boolean multiThreaded = true;
	private JestClient client;


	public void init() {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder(ElasticsearchAddress)
		.multiThreaded(multiThreaded).build());
		client = factory.getObject();
	}
	

	public void createIndex() {
		String settings = "\"settings\" : {\n" +
				"        \"number_of_shards\" : 5,\n" +
				"        \"number_of_replicas\" : 1\n" +
				"    }\n";

		try {
			JestResult result = client.execute(new CreateIndex.Builder(twitterIndex)
			.settings(ImmutableSettings.builder()
					.loadFromSource(settings).build()
					.getAsMap()).build());
			System.out.println(result.toString());
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}
