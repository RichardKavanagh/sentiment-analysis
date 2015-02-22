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
 * The bolt that calaulates the amount of negative words in a String.
 * 
 * @author Richard Kavanagh
 */
public class NegativeWordsBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 42543534L;
	private static final String NEGATIVE_WORDS = "negative-words.txt";

	public void execute(Tuple input, BasicOutputCollector collector) {

		String text = input.getString(input.fieldIndex("tweet_message"));
		Set<String> negativeWords = new HashSet<String>();
		
		try {
			negativeWords = getNegativeWords();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String[] words = text.split(" ");
		int totalWordCount = words.length;
		int negativeWordCount = 0;

		for (String word : words) {
			if (negativeWords.contains(word))
				negativeWordCount++;
		}

		collector.emit(new Values(text , (float) negativeWordCount / totalWordCount));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_message", "negative_score"));
	}

	/*
	 * Reads the negative words from the resources folder and returns as a set.
	 */
	private Set<String> getNegativeWords() throws FileNotFoundException, IOException {

		Set<String> set = new HashSet<String>();
		BufferedReader bufferReader = new BufferedReader(new FileReader(NEGATIVE_WORDS));
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