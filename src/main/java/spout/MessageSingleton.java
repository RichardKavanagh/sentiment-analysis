package spout;

/*
 * @author Richard Kavanagh.
 */
public class MessageSingleton {
	
	private static MessageSingleton instance = null;
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
	}
	
	public String gettMessage() {
		return message;
	}
}