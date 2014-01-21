package storm.ubiquitous.test;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import storm.ubiquitous.bolts.BatchCount.CountValue;
import storm.ubiquitous.state.RedisMap;

@SuppressWarnings({ "unchecked" })
public class TestMap implements Serializable {
	/**
	 * @author aniket
	 */
	private static final long serialVersionUID = -1114459005246460823L;

	public static void main(String[] args) {
		Map<String, CountValue> counters =null;
		RedisMap mapStore = new RedisMap("localhost");
		try {
			//Obtain the state of Bolt for Txid: 3
			BigInteger a=new BigInteger("3");
			byte key[]=a.toByteArray();
			
			counters = (ConcurrentHashMap<String, CountValue>) mapStore.getState(key);
			System.out.println("Object De-Serialized "+ counters.size());

	      	}
	      	catch(Exception i)
	      	{
	         	i.printStackTrace();	         
	      	}	
		
		for(Map.Entry<String, CountValue> entry : counters.entrySet()){
			System.out.println("String : "+entry.getKey()+" Count: "+entry.getValue().count);
		}
	}
	
}
