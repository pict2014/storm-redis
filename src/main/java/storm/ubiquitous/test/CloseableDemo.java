package storm.ubiquitous.test;

import java.io.Closeable;
import java.io.IOException;

public class CloseableDemo implements Closeable{

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try (CloseableDemo c =new CloseableDemo()){
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Closing");
		
	}

}
