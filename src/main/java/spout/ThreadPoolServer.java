package spout;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import twitter4j.Status;

/*
 * A thread pool to handle incoming logstash connections.
 * 
 * @author Richard Kavanagh.
 */
public class ThreadPoolServer implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(ThreadPoolServer.class);
	
	protected int SERVER_PORT = 0;
	protected int THREAD_AMOUNT = 20;

	protected ServerSocket serverSocket = null;
	protected Thread runningThread = null;
	protected boolean isStopped = false;

	protected ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_AMOUNT);

	public ThreadPoolServer(int port, int threads){
		this.SERVER_PORT = port;
		this.THREAD_AMOUNT = threads;
	}

	public void run(){

		synchronized(this){
			this.runningThread = Thread.currentThread();
		}

		openServerSocket();

		while(!isStopped()){
			Socket clientSocket = null;
			try {
				clientSocket = this.serverSocket.accept();
			} catch (IOException err) {

				if(isStopped()) {
					LOGGER.info("Server Stopped.") ;
					break;
				}
				throw new RuntimeException("Error accepting client connection", err);
			}
			this.threadPool.execute(new LogStashClient(clientSocket, "Thread Pool Server"));
		}

		this.threadPool.shutdown();
		LOGGER.info("Server Stopped.") ;
	}

	public synchronized void stop(){
		this.isStopped = true;
		try {
			this.serverSocket.close();
		} catch (IOException err) {
			throw new RuntimeException("Error closing server", err);
		}
	}

	private synchronized boolean isStopped() {
		return this.isStopped;
	}

	private void openServerSocket() {
		try {
			this.serverSocket = new ServerSocket(this.SERVER_PORT);
		} catch (IOException err) {
			throw new RuntimeException("Cannot open port " + SERVER_PORT, err);
		}
	}

	/*
	 * Inner static class to define worker for each logstash client.
	 */
	private static class LogStashClient implements Runnable {

		protected Socket clientSocket = null;

		public LogStashClient(Socket clientSocket, String serverText) {
			this.clientSocket = clientSocket;
		}

		public void run() {
			try {
				ObjectInputStream inputStream  = new ObjectInputStream(clientSocket.getInputStream());
				Status status = (Status) inputStream.readObject();
				if (status != null) {
					MessageSingleton.getInstance().setMessage(status);
				}
				inputStream.close();
				
			} catch (IOException err) {
				/* We swallow the EOF exception here as it is defined behaviour for readObject method. */
			}
			catch (ClassNotFoundException err) {
				err.printStackTrace();
			}
		}
	}
}