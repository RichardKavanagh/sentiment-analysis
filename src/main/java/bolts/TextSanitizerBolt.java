package bolts;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import utils.FileUtils;
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
 * The TextSanitizerBolt bolt that provides further sanitization by stripping away stemming/stop words/suffixes.
 * 
 * @author Richard Kavanagh
 */
public class TextSanitizerBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(TextSanitizerBolt.class);
	private static final long serialVersionUID = 4349364405881264772L;
	
	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached Text sanitizer bolt.");
		try {
			Set<String> stopWords = FileUtils.getStopWords();
			String filteredMessage = input.getString(input.fieldIndex(FieldValue.MESSAGE.getString()));
			for (String word : stopWords) {
					filteredMessage = filteredMessage.replaceAll("\\b" + word + "\\b", "");
			}
			
			collector.emit(new Values(filteredMessage));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}   

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.MESSAGE.getString()));
	}
}