package bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/*
 * The preprocesseer bolt that recieves tweets from the logstash spout.
 * 
 * @author Richard Kavanagh
 */
public class TwitterFilterBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 7432280938048906081L;
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		System.out.println("Recieved tweet at bolt " + tuple.toString());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}   
}