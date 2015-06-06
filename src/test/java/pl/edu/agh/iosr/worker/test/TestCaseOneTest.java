package pl.edu.agh.iosr.worker.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.worker.OpenTSDBWorker;

public class TestCaseOneTest {

	private final String successMessage = "Succesfully performed requests";

	@Test
	public void testValidMessage(){
		String [] args = new String[]{"mode=TEST1","address=172.17.84.76:7001",
				"metric=mem.usage.perc","file=D:/IOSR/data/data2.txt","delay=1000",
				"req_nr=1","tags=cpu:00","aggregator=sum","start=1429833600","end=1430006400"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new TestCaseOne(config);
		worker.run();
		
		assertEquals(successMessage, worker.getResultMessage());
	}
	
}
