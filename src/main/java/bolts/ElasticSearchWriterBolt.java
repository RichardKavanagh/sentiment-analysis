package bolts;


import java.util.HashSet;

import org.apache.log4j.Logger;

import elasticsearch.ElasticsearchClient;
import backtype.storm.task.TopologyContext;
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
	
	private boolean sentimenetJoined = false;
	private boolean idJoined = false;
	String id,text,sentiment,hashtags = "";
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		elasticsearchClient = new ElasticsearchClient();
		if (input.contains("tweet_id")) {
			LOGGER.info("In tweet sentiment clause.");
			id = input.getString((input.fieldIndex("tweet_id")));
			text = input.getString((input.fieldIndex("tweet_message")));
			hashtags = input.getString((input.fieldIndex("tweet_hashtags")));
			idJoined = true;
			if (sentimenetJoined) {
				elasticsearchClient.write(id,text,hashtags,sentiment);
				resetFlags();
			}
		}
		else if (input.contains("tweet_sentiment")) {
			LOGGER.info("In tweet id clause.");
			sentiment = input.getString((input.fieldIndex("tweet_sentiment")));
			sentimenetJoined = true;
			if (idJoined) {
				elasticsearchClient.write(id,text,hashtags,sentiment);
				resetFlags();
			}
		}
		else {
			LOGGER.info("Unknown error occured joining bolts");
		}
	}
	
	private void resetFlags() {
		idJoined = false;
		sentimenetJoined = false;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}