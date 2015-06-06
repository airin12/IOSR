package pl.edu.agh.iosr.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import pl.edu.agh.iosr.config.Configuration;

public class FileLoaderWorkerTest {

	private final String successMessage = "Worker ended job without errors";
	private final String fileNotFoundMessage = "File xxz.txt does not exist";
	private final String noTagsMessage = "Error while reading lines from file:  You must specify at least one tag";
	
	@Test
	public void testValidMessage(){
		String [] args = new String[]{"mode=LOAD","metric=mem.usage.perc","address=172.17.84.76:2002",
				  "tags=cpu:01","file=D:/IOSR/data/data.txt","separator=:","format=value:::timestamp"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new FileLoaderWorker(config);
		worker.run();
		
		assertEquals(successMessage, worker.getResultMessage());
	}
	
	@Test
	public void testFileNotFound(){
		String [] args = new String[]{"mode=LOAD","metric=mem.usage.perc","address=172.17.84.76:2002",
				  "tags=cpu:01","file=xxz.txt","separator=:","format=value:::timestamp"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new FileLoaderWorker(config);
		worker.run();
		
		assertEquals(fileNotFoundMessage, worker.getResultMessage());
	}
	
	@Test
	public void testNoTags(){
		String [] args = new String[]{"mode=LOAD","metric=mem.usage.perc","address=172.17.84.76:2002",
				  "file=D:/IOSR/data/data.txt","separator=:","format=value:::timestamp"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new FileLoaderWorker(config);
		worker.run();
		
		assertEquals(noTagsMessage, worker.getResultMessage());
	}
	
}
