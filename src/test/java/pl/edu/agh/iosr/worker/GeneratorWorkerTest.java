package pl.edu.agh.iosr.worker;

import static org.junit.Assert.*;

import org.junit.Test;

import pl.edu.agh.iosr.config.Configuration;

public class GeneratorWorkerTest {

	private final String successMessage = "Worker ended job without errors";
	
	@Test
	public void testValidMessage(){
		String [] args = new String[]{"mode=GENERATE","metric=mem.usage.perc","tsdb_address=172.17.84.76:2002",
				  "tags=cpu:01","req_nr=1"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new GeneratorWorker(config);
		worker.run();
		
		assertEquals(successMessage, worker.getResultMessage());
	}
	
	@Test
	public void testValidMessageWithTimeStep(){
		String [] args = new String[]{"mode=GENERATE","metric=mem.usage.perc","tsdb_address=172.17.84.76:2002",
				  "tags=cpu:01","req_nr=1","step=1"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new GeneratorWorker(config);
		worker.run();
		
		assertEquals(successMessage, worker.getResultMessage());
	}
}
