package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import twitter4j.Status;
import utils.ConfigurationSingleton;
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
 * Allows or denies access to the topology based on system status configuration.
 * 
 * @author Richard Kavanagh
 */
public class SystemStatusBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(SystemStatusBolt.class);
	private static final long serialVersionUID = -8171045339897756375L;
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached SystemStatusBolt bolt.");
		Status tweet = (Status) input.getValueByField(FieldValue.TWEET.getString());
		if (!systemActive()) {
			LOGGER.info("System is not active, dropping tweet " + tweet.getId());
		}
		else {
			collector.emit(new Values(tweet));
		}
	}
	
	private boolean systemActive() {
		return ConfigurationSingleton.getInstance().getSystemActive();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.TWEET.getString()));
	}
}