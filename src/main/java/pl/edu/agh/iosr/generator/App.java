package pl.edu.agh.iosr.generator;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.worker.FileLoaderWorker;
import pl.edu.agh.iosr.worker.GeneratorWorker;
import pl.edu.agh.iosr.worker.OpenTSDBWorker;

public class App {
	public static void main(String[] args) {

		Configuration config = new Configuration(args);
		if (!config.isValid()) {
			System.out.println(config.getErrorMsg());
			return;
		}

		OpenTSDBWorker runnable = null;

		if (config.getMode().equals(GeneratorWorkModes.GENERATE)) {
			runnable = new GeneratorWorker(config);
		} else if (config.getMode().equals(GeneratorWorkModes.LOAD)){
			runnable = new FileLoaderWorker(config);
		}
		
		Thread thread = new Thread(runnable);

		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(runnable.getResultMessage());
	}
}
