package utils;

import java.io.FileReader;
import java.io.IOException;

import nlp.NLPSentimentAnalyzer;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

/*
 * @author Richard Kavanagh
 */
public class AccuracyMeasure {

	private static final Logger LOGGER = Logger.getLogger(AccuracyMeasure.class);
	
	private static final String DATASET_FILE = "src/main/resources/test_corpus/training.1600000.processed.noemoticon.csv";
	
	private static int correctClassifications = 0;
	private static int incorrectClassifications = 0;
	private static int totalTweets = 0;
	
	public static void main(String [] args) {
		getAccuracyRating();
	}

	public static void getAccuracyRating() {
		LOGGER.info("Reading in test csv file.");
		try {
			CSVReader reader = new CSVReader(new FileReader(DATASET_FILE), ',' , '"' , 0);
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				if (nextLine != null) {
					if (totalTweets == 10000) {
						break;
					}
					totalTweets++;
					String annotatedSentiment = nextLine[0];
					NLPSentimentAnalyzer classifier = new NLPSentimentAnalyzer();
					int sentimentInt = 0;
					sentimentInt = classifier.calculateSentiment(nextLine[5],sentimentInt);
					String calculatedSentiment = Integer.toString(sentimentInt);
					compareSentiments(annotatedSentiment, calculatedSentiment);
				}
			}
		} catch (IOException err) {
			err.printStackTrace();
		}
		System.out.println(correctClassifications);
		System.out.println(incorrectClassifications);
		System.out.println(totalTweets);
	}


	
	

	private static void compareSentiments(String annotatedSentiment,
			String calculatedSentiment) {
		if (annotatedSentiment.equals("0")) {
				if (calculatedSentiment.equals("0") || calculatedSentiment.equals("1")) {
					correctClassifications++;
				}
				else {
					incorrectClassifications++;
				}
		}
		else if(annotatedSentiment.equals("2")) {
			if (calculatedSentiment.equals("2")) {
				correctClassifications++;
			}
			else {
				incorrectClassifications++;
			}
		}
		else {
			if (calculatedSentiment.equals("3") || calculatedSentiment.equals("4")) {
				correctClassifications++;
			}
			else {
				incorrectClassifications++;
			}
		}
	}

	public int calculateSentiment() {
		return 0;
	}
}