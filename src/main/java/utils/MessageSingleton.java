package utils;

import twitter4j.Status;

/*
 * Message singleton used to pass messages safely from the spout.
 * 
 * @author Richard Kavanagh.
 */
public class MessageSingleton {
	
	private static MessageSingleton instance = null;
	
	private boolean available = false;
	private Status message;
	
	/*
	 * Exists only to defeat instantiation.
	 */
	protected MessageSingleton() {
	}
	
	/*
	 * Synchronized to avoid race conditions.
	 */
	public static synchronized MessageSingleton getInstance() {
		if(instance == null) {
			instance = new MessageSingleton();
		}
		return instance;
	}
	
	public void setMessage(Status message) {
		this.message = message;
		available = true;
	}
	
	public Status getMessage() {
		available = false;
		return message;
	}
	
	public boolean availableMessage() {
		return available;
	}
}