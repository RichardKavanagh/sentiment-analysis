package bolts;


import java.util.Map;

import org.apache.log4j.Logger;

import elasticsearch.ElasticSearchConfiguration;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


/*
 * Reads the ELasticSearch config and stores it in a singleton for use throughout the topology.
 * 
 * @author Richard Kavanagh.
 */
public class ElasticSearchConfigurationBolt extends BaseBasicBolt {
	
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchConfigurationBolt.class);
	private static final long serialVersionUID = 2382212674447108118L;
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("In Elasticsearch configuration bolt.");
		ElasticSearchConfiguration config = new ElasticSearchConfiguration();
		System.out.println(config);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	} 
}


