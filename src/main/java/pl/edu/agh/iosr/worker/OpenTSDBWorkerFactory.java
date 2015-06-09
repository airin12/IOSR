package pl.edu.agh.iosr.worker;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.generator.GeneratorWorkModes;
import pl.edu.agh.iosr.worker.test.TestCaseOne;
import pl.edu.agh.iosr.worker.test.TestCaseThreeMain;
import pl.edu.agh.iosr.worker.test.TestCaseTwo;

public class OpenTSDBWorkerFactory {

	public OpenTSDBWorker getOpenTSDBWorker(Configuration config){
		if (config.getMode().equals(GeneratorWorkModes.GENERATE)) {
			return new GeneratorWorker(config);
		} else if (config.getMode().equals(GeneratorWorkModes.LOAD)){
			return new FileLoaderWorker(config);
		} else if (config.getMode().equals(GeneratorWorkModes.TEST1)){
			return new TestCaseOne(config);
		} else if (config.getMode().equals(GeneratorWorkModes.TEST2)){
			return new TestCaseTwo(config);
		} else if (config.getMode().equals(GeneratorWorkModes.TEST3)){
			return new TestCaseThreeMain(config);
		}
		
		return null;
	}
	
}
