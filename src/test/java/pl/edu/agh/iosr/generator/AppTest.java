package pl.edu.agh.iosr.generator;

import static org.junit.Assert.*;

import org.junit.Test;

public class AppTest {

	@Test
	public void testRunApp(){
		String [] args = new String[]{"mode=GENERATE","metric=mem.usage.perc","address=172.17.84.76:2002",
				  "tags=cpu:01","req_nr=1"};
		
		App.main(args);
		assertTrue(true);
	}
	
}
