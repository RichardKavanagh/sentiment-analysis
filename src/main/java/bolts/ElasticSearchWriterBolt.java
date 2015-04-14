package bolts;

import org.apache.log4j.Logger;

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
	
	private ElasticsearchClient elasticsearchClient;
	private boolean sentimenetJoined,idJoined,entityJoined = false;
	String id,user,text,location,links,media,sentiment,hashtags = "";
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		elasticsearchClient = new ElasticsearchClient();
		if (input.contains("tweet_id")) {
			LOGGER.info("In tweet sentiment clause.");
			id = input.getString((input.fieldIndex("tweet_id")));
			user = input.getString(input.fieldIndex("tweet_user"));
			text = input.getString((input.fieldIndex("tweet_message")));
			hashtags = input.getString((input.fieldIndex("tweet_hashtags")));
			idJoined = true;
			if (sentimenetJoined && entityJoined) {
				writeToElasticsearch();
				resetFlags();
			}
		}
		else if (input.contains("tweet_sentiment")) {
			LOGGER.info("In tweet id clause.");
			sentiment = input.getString((input.fieldIndex("tweet_sentiment")));
			sentimenetJoined = true;
			if (idJoined && entityJoined) {
				writeToElasticsearch();
				resetFlags();
			}
		}
		else {
			LOGGER.info("In tweet entity clause.");
			links = input.getString(input.fieldIndex("tweet_URLs"));
			location = input.getString(input.fieldIndex("tweet_location"));
			entityJoined = true;
			if (sentimenetJoined && idJoined) {
				writeToElasticsearch();
				resetFlags();
			}
		}
	}

	private void writeToElasticsearch() {
		String message = text.replaceAll(links, "");
		elasticsearchClient.write(id,user,location,text,links,hashtags,sentiment);
	}
	
	private void resetFlags() {
		idJoined = false;
		sentimenetJoined = false;
		entityJoined = false;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}