package bolts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/*
 * A general File utilites class to handle multiple file related methods.
 * 
 * @author Richard Kavanagh.
 */
public class FileUtils {
	
	
	private static final String POSITIVE_WORDS = "positive-words.txt";
	private static final String NEGATIVE_WORDS = "negative-words.txt";

	
	public static Set<String> getWords(boolean positiveWords) throws FileNotFoundException, IOException {

		Set<String> set = new HashSet<String>();
		BufferedReader bufferReader;
		if(positiveWords) {
			bufferReader = new BufferedReader(new FileReader(POSITIVE_WORDS));
		}
		else {
			bufferReader = new BufferedReader(new FileReader(NEGATIVE_WORDS));
		}
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