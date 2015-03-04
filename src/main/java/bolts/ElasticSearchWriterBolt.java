package bolts;

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

	private static final long serialVersionUID = -4229629366537572766L;

	public void execute(Tuple input, BasicOutputCollector collector) {

		/*
		 * 
		 * Will write sentiment analysis results to Elasticsearch database.
		 * 
		 */

		System.out.println("Reached ElasticSearch bolt.");

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}