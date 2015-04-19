package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import elasticsearch.ElasticSearchConfiguration;
import twitter4j.Status;
import values.FieldValue;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * This bolt resets the topology configuration..
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
		ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
		esConfig.setConfiguration();
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		collector.emit(new Values(tweet));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.TWEET.getString()));
	} 
}