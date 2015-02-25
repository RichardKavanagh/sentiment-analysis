package spout;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import bolts.FileUtils;

/*
 * Client class to send messages from TwitterRiver to Logstash.
 * 
 * @author Richard Kavanagh.
 */
public class TwitterRiverClient {

	private String hostname;
	private int port;

	public TwitterRiverClient(String hostName, int port) {
		this.hostname = hostName;
		this.port = port;
	}

	private DataOutputStream output;

	public synchronized void sendToPort(String tweet) throws IOException {
		Socket socket = new Socket(hostname, port);
		output = new DataOutputStream(socket.getOutputStream());
		if (FileUtils.supportedEncoding(tweet)) {
			output.writeBytes(tweet);
		}
		socket.close();
	}
}