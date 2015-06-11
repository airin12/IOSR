package pl.edu.agh.iosr.worker.test;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Random;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.data.DataSample;
import pl.edu.agh.iosr.http.HTTPRequestSender;
import pl.edu.agh.iosr.worker.OpenTSDBWorker;

import com.google.gson.Gson;

public class TestCaseThreeHelperThread implements OpenTSDBWorker {
	private DataSample templateSample;
	private final Random random = new Random(new Date().getTime());
	private HTTPRequestSender sender;
	private Configuration config;
	private String message;
	private int count = 0;

	public TestCaseThreeHelperThread(Configuration config) {
		this.config = config;
		this.sender = new HTTPRequestSender("http://"+config.getTsdbServiceAddress()+"/api/put");
		this.templateSample = new DataSample(config.getMetric(), new Date().getTime(), config.getMax(), config.getTags());
	}


	public void run() {
		Gson gson = new Gson();

		FileOutputStream fos;
		try {
			fos = new FileOutputStream(config.getFile().replace(".txt", "_2.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			message = "Error while reading file "+config.getFile();
			return;
		}
	 
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		
		for (int i = 0; i < config.getNumberOfRequests(); i++) {
			templateSample.setTimestamp(new Date().getTime());

			for (int j = 0; j < config.getDuplicate(); j++) {
				randomizeTemplete();
				String json = gson.toJson(templateSample);
				System.out.println(" Sending: " + json);
				int responseCode = sender.sendDataSample(json);
				count++;
				long currentTime = new Date().getTime();
				try {
					bw.write(String.valueOf(currentTime)+";"+String.valueOf(count));
					bw.newLine();
					bw.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}

				System.out.println(" Response code: " + responseCode);
			}

			Thread.currentThread();
			try {
				Thread.sleep(config.getDelay());
			} catch (InterruptedException e) {
				message = "Error while performing sleep operation: "+e.getMessage();
			}
		}
		
		try {
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		message = "Worker ended job without errors";
	}

	private void randomizeTemplete() {
		templateSample.setValue(config.getMin() + (config.getMax() - config.getMin()) * random.nextDouble());
	}

	public String getResultMessage() {
		return message;
	}

}
