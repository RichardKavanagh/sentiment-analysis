package topology;

import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import nlp.NLPSentimentAnalyzer;
import au.com.bytecode.opencsv.CSVReader;
import bayes.NaiveBayesClassifier;

/*
 * Calculates the accuracy of the NLPSentimentAnalyzer & NaiveBayesClassifier class's.
 * 
 * @author Richard Kavanagh.
 */
public class ClassifierAccuracy {
	

	private static final Logger LOGGER = Logger.getLogger(ClassifierAccuracy.class);
	
	private static String BAYES = "bayes";
	private static String NLP = "nlp";
	private static final char TAB = '\t';
	
	private static String TEST_SET = "/home/richard/workspace/sentiment-analysis/topology/src/main/resources/accuracy/test-tweets.txt";
	
	private static NaiveBayesClassifier bayesClassifier;
	private static NLPSentimentAnalyzer nlpClassifier;
	
	public static void main(String [] args) {
		LOGGER.info("Launching ClassifierAccuracy class.");
		if (!validCMDLineArgs(args)) {
			LOGGER.info("Invalid classifier selected.");
			System.exit(-1);
		}
		if (args[0].equals(BAYES)) {
			bayesClassifier = new NaiveBayesClassifier(false);
			testAccuracy();
		}
		else if (args[0].equals(NLP)) {
			nlpClassifier = new NLPSentimentAnalyzer();
			testAccuracy();
		}
	}
	
	
	public static void testAccuracy() {
		try {
			CSVReader reader = new CSVReader(new FileReader(TEST_SET), TAB , '"' , 0);
			String[] nextLine;
			double correctPositives = 0; double wrongPositives = 0; 
			double correctNegatives = 0; double wrongNegatives = 0;
			double disregardedTweets = 0;
			
			while ((nextLine = reader.readNext()) != null) {
				String status = nextLine[1]; 
				String annotatedSetiment = nextLine[0]; 
				int sentimnet = 0;
				
				String bayesSentimnet = Integer.toString(bayesClassifier.classifyText(status));
				
				if (annotatedSetiment.equals("0")) {
					/*
					 * If annotated sentiment is negative, then check bayes result.
					 */
					if (bayesSentimnet.equals("0")) {
						correctNegatives++;
					}
					else if (bayesSentimnet.equals("1")) {  
						wrongNegatives++;
					}
				}
				else if (annotatedSetiment.equals("1")) {
					/*
					 * If annotated sentiment is positive, then check bayes result.
					 */
					if (bayesSentimnet.equals("1")) {
						correctPositives++;
					}
					if (bayesSentimnet.equals("0")) {
						wrongPositives++;
					}
				}
			}
			
			double totalTweets = correctPositives + correctNegatives + wrongPositives + wrongNegatives;
			double totalUsableTweets = totalTweets;
			double totalCorrectRatings = correctPositives + correctNegatives;
			double totalWrongRatings = wrongPositives + wrongNegatives;
			double accuracy = totalCorrectRatings / totalUsableTweets;
			System.out.println(totalTweets);
			System.out.println(totalUsableTweets);
			System.out.println(totalCorrectRatings);
			System.out.println(totalWrongRatings);
			System.out.println(disregardedTweets);
			System.out.println("NLPSentimentAnalyzer accuracy rating:");
			System.out.println(accuracy + " %");
			
		} catch (IOException err) {
			err.printStackTrace();
		}
	}
	
	

	private static boolean validCMDLineArgs(String [] args) {
		return args != null && args.length > 0;
	}
}
