package spout;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * A thread pool to handle incoming logstash connections.
 * 
 * @author Richard Kavanagh.
 */
public class ThreadPoolServer implements Runnable {

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
					System.out.println("Server Stopped.") ;
					break;
				}
				throw new RuntimeException("Error accepting client connection", err);
			}
			this.threadPool.execute(new LogStashClient(clientSocket, "Thread Pool Server"));
		}

		this.threadPool.shutdown();
		System.out.println("Server Stopped.") ;
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
				InputStream input  = clientSocket.getInputStream();
				long time = System.currentTimeMillis();
				input.close();
				System.out.println("Request processed: " + time);
				
			} catch (IOException err) {
				throw new RuntimeException("Error closing server", err);
			}
		}
	}
}