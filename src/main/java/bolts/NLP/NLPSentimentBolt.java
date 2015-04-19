package bolts.NLP;

import org.apache.log4j.Logger;

import values.FieldValue;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

/*
 * This bolt will perform sentiment analysis usign stanfords openNLP library.
 * 
 * @author Richard Kavanagh.
 */
public class NLPSentimentBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(NLPSentimentBolt.class);
	private static final long serialVersionUID = -422962936423672766L;

	private static StanfordCoreNLP pipeline;
	private static String PIPELINE_PROPERTIES = "PipeLine.properties";

	/*
	 *  The base model is pre-trained on ~12000 sentences on a recursive neural network.
	 *  Achieves 64% accuracy.
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		LOGGER.info("Reached NLPSentimentBolt.");
		pipeline = new StanfordCoreNLP(PIPELINE_PROPERTIES);
		String tweet = input.getString(input.fieldIndex(FieldValue.MESSAGE.getString()));
		
		int mainSentiment = 0;
		if (tweet != null && tweet.length() > 0) {
			mainSentiment = calculateSentiment(tweet, mainSentiment);
			collector.emit(new Values(mainSentiment));
		}
	}

	private synchronized int calculateSentiment(String tweet, int mainSentiment) {
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

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FieldValue.SCORE.getString()));
	}
}