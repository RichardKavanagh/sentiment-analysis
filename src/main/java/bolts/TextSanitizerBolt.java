package bolts;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import topology.FileUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The TextSanitizerBolt bolt that provides further sanitization by stripping away stemming/stop words/suffixes.
 * 
 * @author Richard Kavanagh
 */
public class TextSanitizerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 4349364405881264772L;

	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {

		try {
			Set<String> stopWords = FileUtils.getStopWords();
			String message = input.getString(input.fieldIndex("tweet_message"));
			for (String word : stopWords) {
				message = message.replaceAll("\\b" + word + "\\b", "");
			}
			collector.emit(new Values(input.getString(input.fieldIndex("tweet_id")),
									  input.getString(input.fieldIndex("tweet_user")),
									  message));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}   

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id","tweet_user", "tweet_message"));
	}
}