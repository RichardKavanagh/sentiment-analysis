package bolts;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import topology.FileUtils;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The bolt that calaulates the amount of negative words in a String.
 * 
 * @author Richard Kavanagh
 */
public class NegativeWordsBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 42543534L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String text = input.getString(input.fieldIndex("tweet_message"));
		String id = input.getString(input.fieldIndex("tweet_id"));
		Set<String> negativeWords = new HashSet<String>();
		
		try {
			negativeWords = FileUtils.getWords(false);
		} catch (IOException err) {
			err.printStackTrace();
		}

		String[] words = text.split(" ");
		int negativeWordCount = 0;
		for (String word : words) {
			if (negativeWords.contains(word)) {
				negativeWordCount++;
			}
		}
		collector.emit(new Values(id, text, negativeWordCount));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id","tweet_message", "negative_word_score"));
	}
}