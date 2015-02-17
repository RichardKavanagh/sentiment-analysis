package spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*
 * Spout that listens for logstash tcp output at specific port.
 * 
 * @author Richard Kavanagh.
 */
public class LogStashSpout extends BaseRichSpout  {

	private static final long serialVersionUID = -7279996556144453244L;
	private SpoutOutputCollector collector;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector ) {
		this.collector = collector;
	}

	public void nextTuple() {
		MessageSingleton ms = MessageSingleton.getInstance();
		if (ms.availableMessage()) {
			collector.emit( new Values(ms.gettMessage()));
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
		declarer.declare(new Fields("json-tweet"));
	}
}