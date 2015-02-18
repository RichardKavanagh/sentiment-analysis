package bolts;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.commons.lang.StringUtils;

/*
 * The preprocesseer bolt that provides first-round data sanitization.
 * 
 * @author Richard Kavanagh
 */
public class TextPreprocessorBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 5324264730654714029L;

	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String message = input.getString(input.fieldIndex("tweet_message"));
		String user = input.getString(input.fieldIndex("tweet_user"));
		message = clean(message);
		collector.emit(new Values(user, message));
	}

	/**
	 * The method first normalizes a String, then converts it to lowercase and removes ASCII characters, which might be problematic
	 * all whitespaces, dots ('.') ,(semi-)colons (';' and ':'), equals ('=') ,ampersands ('&')
	 * slashes ('/') and angle brackets ('<' and '>').
	 */
	private String clean(String input) {
		String result = normalize(input);
		result = StringUtils.lowerCase(result);
		result = result.replaceAll("[\\s.:;&=<>/]", "");
		return result;
	}

	private String normalize(String input) {
		input = Normalizer.normalize(input, Form.NFD).replaceAll("[^\\p{ASCII}]", "");
		return input;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_message", "tweet_user"));
	}
}

