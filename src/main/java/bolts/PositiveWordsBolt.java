package bolts;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import topology.FileUtils;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * The bolt that calaulates the amount of positive words in a String.
 * 
 * @author Richard Kavanagh
 */
public class PositiveWordsBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = -4229629366537572766L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String text = input.getString(input.fieldIndex("tweet_message"));
		Set<String> positiveWords = new HashSet<String>();
		
		try {
			positiveWords = FileUtils.getWords(true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		String[] words = text.split(" ");
		int positiveWordCount = 0;
		for (String word : words) {
			if (positiveWords.contains(word))
				positiveWordCount++;
		}
		collector.emit(new Values(text, positiveWordCount));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("tweet_message", "positive_word_score"));
	}
}