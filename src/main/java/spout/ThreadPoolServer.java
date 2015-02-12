package spout;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * A thread pool to handle incoming logstash connections.
 * 
 * @author Richard Kavanagh.
 */
public class ThreadPoolServer implements Runnable {


	protected int SERVER_PORT = 6789;
	protected int THREAD_AMOUNT = 10;

	protected ServerSocket serverSocket = null;
	protected boolean isStopped = false;
	protected Thread runningThread = null;

	protected ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_AMOUNT);

	public ThreadPoolServer(int port){
		this.SERVER_PORT = port;
	}

	public void run(){

		synchronized(this){
			this.runningThread = Thread.currentThread();
		}

		openServerSocket();

		while(! isStopped()){
			Socket clientSocket = null;
			try {
				clientSocket = this.serverSocket.accept();
			} catch (IOException e) {

				if(isStopped()) {
					System.out.println("Server Stopped.") ;
					break;
				}
				throw new RuntimeException("Error accepting client connection", e);
			}
			this.threadPool.execute(new WorkerRunnable(clientSocket, "Thread Pool Server"));
		}

		this.threadPool.shutdown();
		System.out.println("Server Stopped.") ;
	}


	private synchronized boolean isStopped() {
		return this.isStopped;
	}

	public synchronized void stop(){
		this.isStopped = true;
		try {
			this.serverSocket.close();
		} catch (IOException e) {
			throw new RuntimeException("Error closing server", e);
		}
	}

	private void openServerSocket() {
		try {
			this.serverSocket = new ServerSocket(this.SERVER_PORT);
		} catch (IOException e) {
			throw new RuntimeException("Cannot open port " + SERVER_PORT, e);
		}
	}
	
	/*
	 * Inner thread class to read connection.
	 */
	private static class WorkerRunnable implements Runnable{

		protected Socket clientSocket = null;
		protected String serverText   = null;

		public WorkerRunnable(Socket clientSocket, String serverText) {
			this.clientSocket = clientSocket;
			this.serverText   = serverText;
		}

		public void run() {
			try {
				InputStream input  = clientSocket.getInputStream();
				OutputStream output = clientSocket.getOutputStream();
				long time = System.currentTimeMillis();
				output.write(("").getBytes());
				output.close();
				input.close();
				System.out.println("Request processed: " + time);
			} catch (IOException e) {
				//report exception somewhere.
				e.printStackTrace();
			}
		}
	}
}