package bolts;

import org.apache.log4j.Logger;

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

	private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
	private static final long serialVersionUID = -4229629366537572766L;

	public void execute(Tuple input, BasicOutputCollector collector) {

		LOGGER.info("In Elasticsearch bolt.");
		
		/*
		ElasticSearchClient client = new ElasticSearchClient();
		client.init();
		client.createIndex();
		*/
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
