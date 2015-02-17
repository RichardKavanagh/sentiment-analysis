package spout;

/*
 * @author Richard Kavanagh.
 */
public class MessageSingleton {
	
	private static MessageSingleton instance = null;
	private boolean available = false;
	private String message = "";
	
	/*
	 *  Exists only to defeat instantiation.
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
	
	public void setMessage(String message) {
		this.message = message;
		available = true;
	}
	
	public String gettMessage() {
		available = false;
		return message;
	}
	
	public boolean availableMessage() {
		return available;
	}
}