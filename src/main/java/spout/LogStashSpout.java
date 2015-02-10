package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;


/*
 * Spout that listens for logstash tcp output at specific port.
 * 
 * @author Richard Kavanagh.
 */
public class LogStashSpout extends BaseRichSpout  {

	private static final long serialVersionUID = -7279996556144453244L;
	private SpoutOutputCollector collector;

	private static int tweets = 1;

	public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) {
		this.collector = collector;
	}

	/*
	 * TODO Implement multi-threaded or async solution to keep up with logstash rate.
	 */
	public void nextTuple() {

		ServerSocket serverSocket;
		try {
			
			serverSocket = new ServerSocket(6789);
			Socket clientSocket = serverSocket.accept();
			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			
			clientSocket.close();
			tweets++;
			return;
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void ack(Object id) {
	}

	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}