package bayes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.apache.mahout.vectorizer.TFIDF;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

import fields.BayesPaths;
import fields.ModelTrainer;

/*
 * @author Richard Kavanagh.
 */
public class NaiveBayesClassifier {

	private static final Logger LOGGER = Logger.getLogger(NaiveBayesClassifier.class);
	
	@SuppressWarnings("deprecation")
	private static final Version DOCUMENT_EXTRACTOR = Version.LUCENE_46;
	
	private static final String TEXT_FIELD = "text";
	private static final String TAB_STRING = "\t";
	private static final String SLASH = "/";
	private static final String TWEET_APPEND = "/tweet";
	private static final String VECTOR_APPEND = "/tfidf-vectors";
	
	private static final int POSITIVE_RESULT = 1;
	private static final int CARDINALITY = 10000;
	private static final int TEXT_INDEX = 1;

	private static String RAW_DATASET = "/home/richard/Desktop/sentiment-analysis/topology/src/main/resources/exemplar/training.1600000.processed.noemoticon.csv";
	private static String CLEANED_DATASET = "tweets.txt";
	private Configuration bayesConfiguration = new Configuration();

	
	public NaiveBayesClassifier() {
		LOGGER.info("Constructing bayes classifier.");
		convertData();
		inputDataToSequenceFile();
		sequenceFileToSparseVector();
		trainModel();
	}
	
	/*
	 * Only publicly available method for classification.  
	 */
	public int classifyText(String text) {
		Map<String, Integer> dictionary = readDictionary(bayesConfiguration, new Path(BayesPaths.DICTIONARY.getString()));
		Map<Integer, Long> documentFrequency = readDocumentFrequency(bayesConfiguration, new Path(BayesPaths.DOCUMENT_FREQ.getString()));
		Multiset<String> words = ConcurrentHashMultiset.create();
		Analyzer analyzer = new StandardAnalyzer(DOCUMENT_EXTRACTOR);
		TokenStream tokenStream;
		int wordCount = 0;
		try {
			tokenStream = analyzer.tokenStream(TEXT_FIELD, new StringReader(text));
			CharTermAttribute termAttribute = tokenStream .addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				if (termAttribute.length() > 0) {
					String word = tokenStream.getAttribute(CharTermAttribute.class).toString();
					Integer wordId = dictionary.get(word);
					if (wordId != null) {
						words.add(word);
						wordCount++;
					}
				}
			}
			tokenStream.end();
			tokenStream.close();
		} catch (IOException err) {
			err.printStackTrace();
		}


		int documentCount = documentFrequency.get(-1).intValue();

		/*
		 *  Create a vector for the new test.
		 */
		Vector vector = new RandomAccessSparseVector(CARDINALITY);
		TFIDF termFrequencyInverseDocument = new TFIDF();

		for (Multiset.Entry<String> entry : words.entrySet()) {
			String word = entry.getElement();
			int count = entry.getCount();
			Integer wordId = dictionary.get(word);
			Long freq = documentFrequency.get(wordId);
			double termFrequencyValue = termFrequencyInverseDocument.calculate(count, freq.intValue(),wordCount, documentCount);
			vector.setQuick(wordId, termFrequencyValue);
		}
		return createClassifier(analyzer, vector);
	}


	/*
	 * Transforms the raw training set.
	 */
	private void convertData() {
		LOGGER.info("Converting raw data set.");
		try {
			CSVReader reader = new CSVReader(new FileReader(RAW_DATASET), ',' , '"' , 0);
			Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(CLEANED_DATASET, true), "UTF-8"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				if (nextLine != null) {
					String sentiment = nextLine[0];
					String message = nextLine[5];
					if (sentiment.equals("0")) {
						writer.write("0" + TAB_STRING + message);
					} else if (sentiment.equals("4")) {
						writer.write("1" + TAB_STRING + message);
					} else {
					}
					writer.write("\n");
				}
			}
		} catch (IOException err) {
			err.printStackTrace();
		}
	}
	

	/*
	 * This inputs the data to a sequence file
	 */
	private void inputDataToSequenceFile()  {
		LOGGER.info("Inputing data to sequence file format.");
		BufferedReader bufferedReader = null;
		FileSystem fileSystem = null;
		Path seqFilePath = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(CLEANED_DATASET));
			fileSystem = FileSystem.getLocal(bayesConfiguration);
			seqFilePath = new Path(BayesPaths.SEQUENCE_FILE.getString());
			fileSystem.delete(seqFilePath, false);
		} catch (FileNotFoundException err) {
			err.printStackTrace();
		} catch (IOException err) {
			err.printStackTrace();
		}


		SequenceFile.Writer writer;
		try {
			writer = SequenceFile.createWriter(fileSystem, bayesConfiguration, seqFilePath, Text.class, Text.class);
			int count = 0;
			try {
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					String[] tokens = line.split(TAB_STRING);
					count++;
					try {
					 writer.append(new Text(SLASH + tokens[0] + TWEET_APPEND + count),
							new Text(tokens[1]));
					}
					catch(ArrayIndexOutOfBoundsException err) {}
				}
			} finally {
				bufferedReader.close();
				writer.close();
			}
		} catch (IOException err) {
			err.printStackTrace();
		}
	}


	/*
	 *  Uses the previously created sequence file to create SparseVectors.
	 *  These vectors contain the TFIDF measurement for the words in the tweets
	 *  and will be used to train the classifier
	 */
	private void sequenceFileToSparseVector() {
		LOGGER.info("Creating sparse vectors for TFIDF measurement.");
		SparseVectorsFromSequenceFiles sparseVectors = new SparseVectorsFromSequenceFiles();
		try {
			sparseVectors.run(new String[] {ModelTrainer.INPUT_DIR.getString(), BayesPaths.SEQUENCE_FILE.getString(),
					ModelTrainer.OUTPUT_DIR.getString(), BayesPaths.VECTOR.getString(),
					ModelTrainer.OVERWRITE_OUTPUT.getString() });
		} catch (Exception err) {
			err.printStackTrace();
		}
	}

	/*
	 *  Creates a new model based on our training set.
	 */
	private void trainModel() {
		LOGGER.info("Creating new model based on training set.");
		TrainNaiveBayesJob trainingJob = new TrainNaiveBayesJob();
		trainingJob.setConf(bayesConfiguration);
		try {
			trainingJob.run(new String[] { ModelTrainer.INPUT_DIR.getString(), BayesPaths.VECTOR.getString() + VECTOR_APPEND,
					ModelTrainer.OUTPUT_DIR.getString(), BayesPaths.MODEL.getString(),
					ModelTrainer.LABEL_INDEX.getString(), BayesPaths.LABEL_INDEX.getString(),
					ModelTrainer.EXTRACT_LABELS.getString(), ModelTrainer.TRAIN_COMPLEMENTARY.getString(),
					ModelTrainer.OVERWRITE_OUTPUT.getString() });
		} catch (Exception err) {
			err.printStackTrace();
		}
	}

	private int createClassifier(Analyzer analyzer, Vector vector) {
		NaiveBayesModel model;
		StandardNaiveBayesClassifier classifier = null;
		try {
			model = NaiveBayesModel.materialize(new Path(BayesPaths.MODEL.getString()), bayesConfiguration);
			classifier = new StandardNaiveBayesClassifier(model);
		} catch (IOException err) {
			err.printStackTrace();
		}
		return calculateSentiment(classifier, analyzer, vector);
	}


	/*
	 *  With the classifier, we get one score for each label.The label with the highest 
	 *  score is the one the tweet is more likely to be associated to our current input
	 */
	private int calculateSentiment(StandardNaiveBayesClassifier classifier, Analyzer analyzer, Vector vector) {
		LOGGER.info("Calculating sentiment.");
		Vector resultVector = classifier.classifyFull(vector);
		double bestScore = -Double.MAX_VALUE;
		int bestCategoryId = -1;
		int categoryId = 0;
		double score = 0;

		for (Element element : resultVector.all())  {
			categoryId = element.index();
			score = element.get();
			if (score > bestScore) {
				bestScore = score;
				bestCategoryId = categoryId;
			}
			if (categoryId == 1) {
				System.out.println("Probability of being positive: " + score);
			} else {
				System.out.println("Probability of being negative: " + score);
			}
		}
		analyzer.close();
		return bestCategoryId;
	}


	private Map<String, Integer> readDictionary(Configuration configuration, Path path) {
		Map<String, Integer> dictionnary = new HashMap<String, Integer>();
		for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(path, true, configuration)) {
			String first = pair.getFirst().toString();
			int second = pair.getSecond().get();
			dictionnary.put(first, second);
		}
		return dictionnary;
	}

	private Map<Integer, Long> readDocumentFrequency(Configuration configuration, Path documentFrequencyPath) {
		Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
		for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(documentFrequencyPath, true, configuration)) {
			int first = pair.getFirst().get();
			long second =  pair.getSecond().get();
			documentFrequency.put(first,second);
		}
		return documentFrequency;
	}
}