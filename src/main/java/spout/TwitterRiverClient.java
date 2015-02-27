package spout;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import topology.FileUtils;
import twitter4j.Status;

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

	private ObjectOutputStream output;

	public synchronized void writeToTopology(Status status) throws IOException {
		Socket socket = new Socket(hostname, port);
		output = new ObjectOutputStream(socket.getOutputStream());
		if (FileUtils.supportedEncoding(status.toString())) {
			output.writeObject(status);
		}
		socket.close();
	}
}