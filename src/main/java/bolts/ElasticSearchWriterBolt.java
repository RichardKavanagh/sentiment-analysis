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
	
	private boolean sentimenetJoined = false;
	private boolean idJoined = false;
	String id,text,sentiment = "";
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		elasticsearchClient = new ElasticsearchClient();
		LOGGER.info("In ElasticSearch writer bolt.");
		
		
		if (input.contains("tweet_id")) {
			LOGGER.info("In tweet sentiment clause.");
			id = input.getString((input.fieldIndex("tweet_id")));
			text = input.getString((input.fieldIndex("tweet_text")));
			idJoined = true;
			if (sentimenetJoined) {
				elasticsearchClient.write(id,text,sentiment);
			}
		}
		else if (input.contains("tweet_sentiment")) {
			LOGGER.info("In tweet id clause.");
			sentiment = input.getString((input.fieldIndex("tweet_sentiment")));
			sentimenetJoined = true;
			if (idJoined) {
				elasticsearchClient.write(id,text,sentiment);
			}
		}
		else {
			LOGGER.info("Unknown error occured joining bolts");
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}