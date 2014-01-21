/**
 * 
 */
package storm.ubiquitous.state;

import java.io.Closeable;


/**
 * @author aniket
 *
 */
public interface IPersistentMap extends Closeable{
	public void setState(byte[] key, Object value);
	public Object getState(byte[] key);	
}
