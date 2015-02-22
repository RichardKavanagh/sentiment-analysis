package bolts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
	private static final String POSITIVE_WORDS = "positive-words.txt";

	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String text = input.getString(input.fieldIndex("tweet_message"));
		Set<String> positiveWords = new HashSet<String>();
		
		try {
			positiveWords = getPositiveWords();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String[] words = text.split(" ");
		int totalWordCount = words.length;
		int positiveWordCount = 0;

		for (String word : words) {
			if (positiveWords.contains(word))
				positiveWordCount++;
		}

		collector.emit(new Values(text , (float) positiveWordCount / totalWordCount));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("tweet_message", "positive_word_score"));
	}
	
	/*
	 * Reads the positive words from the resources folder and returns as a set.
	 */
	private Set<String> getPositiveWords() throws FileNotFoundException, IOException {

		Set<String> set = new HashSet<String>();
		BufferedReader bufferReader = new BufferedReader(new FileReader(POSITIVE_WORDS));
		try {
			String line = bufferReader.readLine();

			while (line != null) {
				set.add(line);
				set.add(System.lineSeparator());
				line = bufferReader.readLine();
			}
		} finally {
			bufferReader.close();
		}
		return set;
	}

	
}
