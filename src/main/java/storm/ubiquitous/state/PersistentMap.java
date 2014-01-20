package storm.ubiquitous.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import redis.clients.jedis.Jedis;

/**
 * This is an idea to build abstractions for bolts with fault-tolerant state (so if a task dies and gets 
 * reassigned to another machine it still has its state).So generally you keep any persistent state in a 
 * database, oftentimes doing something like waiting to ack() tuples until you've done a batch update to 
 * the database. Stateful bolts will just be a much more efficient way of keeping a large amount of 
 * state at hand in a bolt
 * @author aniket
 */
public class PersistentMap {
	
	String serverURL;
	ByteArrayOutputStream byteOut;
	ObjectOutputStream out;
	ByteArrayInputStream byteIn;
	ObjectInputStream in;
	
	public PersistentMap(String serverURL) {
		this.serverURL=serverURL;		
	}

	public void setState(byte[] key, Object value) throws IOException {
		Jedis db = new Jedis(serverURL);
		
		try {
			 
			 db.connect();
	         byteOut = new ByteArrayOutputStream();
			 out = new ObjectOutputStream(byteOut);
	         out.writeObject(value);
	        
	         byte [] byteValue = byteOut.toByteArray();
	         db.set(key,byteValue);		         
	         System.out.println("Bolt state persisted.");
	         out.close();
		     byteOut.close();
	        
	      	}
			
		catch(Exception i) {			
			 i.printStackTrace();	          
	      	}
		finally{
			 db.disconnect();
			}
			
	}
	public Object getState(byte[] key) throws IOException {
		Object value = null;
		Jedis db = new Jedis(serverURL);
		
		try {
			 
			 db.connect();
			 byte[] store = db.get(key);
			 byteIn = new ByteArrayInputStream(store);
			 in = new ObjectInputStream(byteIn);
	         value = in.readObject();
	         System.out.println("Bolt state retrieved.");
	         in.close();
	         byteIn.close();
	        
	      	}
			
		catch(Exception i) {
	         i.printStackTrace();
	      	}
		finally{
			 db.disconnect();
			}
		
		return value;
		
	}
}
