package spout;

import java.util.Map;

import org.apache.log4j.Logger;

import utils.MessageSingleton;
import values.FieldValue;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*
 * Spout that that acts as an entry point into the Topology.
 * 
 * @author Richard Kavanagh.
 */
public class TwitterRiverSpout extends BaseRichSpout  {

	private static final Logger LOGGER = Logger.getLogger(TwitterRiverSpout.class);
	private static final long serialVersionUID = -7279996556144453244L;
	
	private SpoutOutputCollector collector;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector ) {
		this.collector = collector;
	}

	public void nextTuple() {
		MessageSingleton message = MessageSingleton.getInstance();
		if (message.availableMessage()) {
			LOGGER.info("Sending tweet into topology.");
			collector.emit(new Values(message.getMessage()));
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
	}
	
	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.TWEET.getString()));
	}
}