package pl.edu.agh.iosr.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import pl.edu.agh.iosr.config.Configuration;

public class FileLoaderWorkerTest {

	private final String successMessage = "Worker ended job without errors";
	private final String fileNotFoundMessage = "File xxz.txt does not exist";
	private final String noTagsMessage = "Error while reading lines from file:  You must specify at least one tag";
	
	@Test
	public void testValidMessage() throws URISyntaxException{
		Path resourcePath = Paths.get(getClass().getResource("/data.txt").toURI());
		String path = resourcePath.toString();

		String [] args = new String[]{"mode=LOAD","metric=mem.usage.perc","tsdb_address=172.17.84.76:2002",
				  "tags=cpu:01","file="+path,"separator=:","format=value:::timestamp"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new FileLoaderWorker(config);
		worker.run();
		
		assertEquals(successMessage, worker.getResultMessage());
	}
	
	@Test
	public void testFileNotFound(){
		String [] args = new String[]{"mode=LOAD","metric=mem.usage.perc","tsdb_address=172.17.84.76:2002",
				  "tags=cpu:01","file=xxz.txt","separator=:","format=value:::timestamp"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new FileLoaderWorker(config);
		worker.run();
		
		assertEquals(fileNotFoundMessage, worker.getResultMessage());
	}
	
	@Test
	public void testNoTags() throws URISyntaxException{
		Path resourcePath = Paths.get(getClass().getResource("/data.txt").toURI());
		String path = resourcePath.toString();
		
		String [] args = new String[]{"mode=LOAD","metric=mem.usage.perc","tsdb_address=172.17.84.76:2002",
				  "file="+path,"separator=:","format=value:::timestamp"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		
		OpenTSDBWorker worker = new FileLoaderWorker(config);
		worker.run();
		
		assertEquals(noTagsMessage, worker.getResultMessage());
	}
	
}
