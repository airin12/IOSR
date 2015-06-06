package pl.edu.agh.iosr.worker;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.generator.GeneratorWorkModes;
import pl.edu.agh.iosr.worker.test.TestCaseOne;

public class OpenTSDBWorkerFactoryTest {

	private OpenTSDBWorkerFactory factory = new OpenTSDBWorkerFactory();
	
	@Test
	public void testGeneratorWorkerCreation(){
		String[] args = {"mode="+GeneratorWorkModes.GENERATE};
		Configuration config = new Configuration(args);
		OpenTSDBWorker worker = factory.getOpenTSDBWorker(config);
		assertEquals(worker.getClass(),GeneratorWorker.class);
	}
	
	@Test
	public void testLoaderWorkerCreation(){
		String[] args = {"mode="+GeneratorWorkModes.LOAD};
		Configuration config = new Configuration(args);
		OpenTSDBWorker worker = factory.getOpenTSDBWorker(config);
		assertEquals(worker.getClass(),FileLoaderWorker.class);
	}
	
	@Test
	public void testTest1WorkerCreation(){
		String[] args = {"mode="+GeneratorWorkModes.TEST1};
		Configuration config = new Configuration(args);
		OpenTSDBWorker worker = factory.getOpenTSDBWorker(config);
		assertEquals(worker.getClass(),TestCaseOne.class);
	}
	
}
