package pl.edu.agh.iosr.generator;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.worker.OpenTSDBWorker;
import pl.edu.agh.iosr.worker.OpenTSDBWorkerFactory;

public class App {
	public static void main(String[] args) {

		Configuration config = new Configuration(args);
		if (!config.isValid()) {
			System.out.println(config.getErrorMsg());
			return;
		}

		OpenTSDBWorkerFactory factory = new OpenTSDBWorkerFactory();
		
		OpenTSDBWorker runnable = factory.getOpenTSDBWorker(config);
		if(runnable == null){
			System.out.println(" Unknown type of worker ");
			return;
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
