package bolts;

import org.apache.log4j.Logger;

import topology.FieldValue;
import elasticsearch.ElasticsearchClient;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/*
 * The bolt that stores results in ElasticSearch.
 * 
 * @author Richard Kavanagh
 */
public class ElasticSearchWriterBolt extends BaseBasicBolt {

	private static final Logger LOGGER = Logger.getLogger(ElasticSearchWriterBolt.class);
	private static final long serialVersionUID = -4229629366537572766L;
	
	private static final String EMPTY = "";
	private ElasticsearchClient elasticsearchClient;
	private boolean sentimenetJoined,idJoined,entityJoined = false;
	String id,user,text,location,links,media,hashtags,sentiment,score = EMPTY;
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		elasticsearchClient = new ElasticsearchClient();
		if (input.contains(FieldValue.ID.getString())) {
			LOGGER.info("In tweet sentiment clause.");
			readSentimentStream(input);
		}
		else if (input.contains(FieldValue.SENTIMENT.getString())) {
			LOGGER.info("In tweet id clause.");
			readElasticSearchWriteStream(input);
		}
		else {
			LOGGER.info("In tweet entity clause.");
			readEntityStream(input);
		}
	}

	private void readEntityStream(Tuple input) {
		links = input.getString(input.fieldIndex(FieldValue.URL.getString()));
		location = input.getString(input.fieldIndex(FieldValue.LOCATION.getString()));
		entityJoined = true;
		if (sentimenetJoined && idJoined) {
			writeToElasticsearch();
			resetFlags();
		}
	}

	private void readElasticSearchWriteStream(Tuple input) {
		sentiment = input.getString((input.fieldIndex(FieldValue.SENTIMENT.getString())));
		score = Integer.toString(input.getInteger((input.fieldIndex(FieldValue.SCORE.getString()))));
		sentimenetJoined = true;
		if (idJoined && entityJoined) {
			writeToElasticsearch();
			resetFlags();
		}
	}

	private void readSentimentStream(Tuple input) {
		id = input.getString((input.fieldIndex(FieldValue.ID.getString())));
		user = input.getString(input.fieldIndex(FieldValue.USER.getString()));
		text = input.getString((input.fieldIndex(FieldValue.MESSAGE.getString())));
		hashtags = input.getString((input.fieldIndex(FieldValue.HASHTAG.getString())));
		idJoined = true;
		if (sentimenetJoined && entityJoined) {
			writeToElasticsearch();
			resetFlags();
		}
	}

	private void writeToElasticsearch() {
		String message = text.replaceAll(links, EMPTY);
		elasticsearchClient.write(id,user,location,message,links,hashtags,sentiment,score);
	}
	
	private void resetFlags() {
		idJoined = false;
		sentimenetJoined = false;
		entityJoined = false;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}