package nlp;

import org.apache.log4j.Logger;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

/*
 * @author Richard Kavanagh
 */
public class NLPSentimentAnalyzer {
	
	private static final Logger LOGGER = Logger.getLogger(NLPSentimentAnalyzer.class);
	
	private static final String BASE_NLP = "nlp";
	private static String BASE_PIPELINE_PROPERTIES = "PipeLine.properties";
	
	private StanfordCoreNLP pipeline;

	public int calculateSentiment(String status, int sentiment) {
		pipeline = getModelPipeline();
		if (status != null && status.length() > 0) {
			sentiment = getSentiment(status, sentiment);
			return sentiment;
		}
		return -1;
	}

	private StanfordCoreNLP getModelPipeline() {
		LOGGER.info("Getting PipeLine properties.");
		return new StanfordCoreNLP(BASE_PIPELINE_PROPERTIES);
	}
	
	private synchronized int getSentiment(String tweet, int mainSentiment) {
		int longest = 0;
		Annotation annotation = pipeline.process(tweet);
		for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
			Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
			int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			String partText = sentence.toString();
			if (partText.length() > longest) {
				mainSentiment = sentiment;
				longest = partText.length();
			}
		}
		return mainSentiment;
	}
}