package bolts;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import utils.FileUtils;
import values.FieldValue;
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
	
	private static final Logger LOGGER = Logger.getLogger(PositiveWordsBolt.class);
	private static final long serialVersionUID = -4229629366537572766L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String text = input.getString(input.fieldIndex(FieldValue.MESSAGE.getString()));
		Set<String> positiveWords = new HashSet<String>();
		List<String> positiveWordsInTweet = new LinkedList<String>();
		
		try {
			positiveWords = FileUtils.getWords(true);
		} catch (IOException err) {
			err.printStackTrace();
		}

		String[] words = text.split(" ");
		int positiveWordCount = 0;
		for (String word : words) {
			if (positiveWords.contains(word)) {
				positiveWordsInTweet.add(word);
				positiveWordCount++;
			}
		}
		LOGGER.info("Positive words " + positiveWordsInTweet);
		collector.emit(new Values(positiveWordCount));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields(FieldValue.POSITIVE.getString()));
	}
}