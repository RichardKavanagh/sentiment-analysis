package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.mozilla.universalchardet.UniversalDetector;

/*
 * A general File utilites class to handle multiple file related methods.
 * 
 * @author Richard Kavanagh.
 */
public class FileUtils {
	
	private static final String POSITIVE_WORDS = "src/main/resources/words/positive-words.txt";
	private static final String NEGATIVE_WORDS = "src/main/resources/words/negative-words.txt";
	private static final String STOP_WORDS = "src/main/resources/words/stop-words.txt";
	
	private static final String SUPPORTED_ENCODING = "UTF-8";
	private static UniversalDetector detector = new UniversalDetector(null);

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
	
	public static Set<String> getStopWords() throws FileNotFoundException, IOException {
		Set<String> set = new HashSet<String>();
		BufferedReader bufferReader;
		bufferReader = new BufferedReader(new FileReader(STOP_WORDS));
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
	
	public static boolean supportedEncoding(String tweet) {
		byte[] bytes = tweet.getBytes();
		detector.handleData(bytes, 0, bytes.length);
		detector.dataEnd();

		String encoding = detector.getDetectedCharset();
		detector.reset();

		if (encoding == null) {
			return false;
		}
		else if (encoding.equals(SUPPORTED_ENCODING)) {
			return true;
		}
		else {
			return false;
		}
	}
}