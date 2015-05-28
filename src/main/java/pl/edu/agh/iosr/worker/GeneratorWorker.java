package pl.edu.agh.iosr.worker;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.data.DataSample;
import pl.edu.agh.iosr.http.HTTPRequestSender;

import com.google.gson.Gson;

public class GeneratorWorker implements OpenTSDBWorker {
	private DataSample templateSample;
	private final Random random = new Random(new Date().getTime());
	private HTTPRequestSender sender;
	private Configuration config;
	private String message;

	public GeneratorWorker(Configuration config) {
		this.config = config;
		this.sender = new HTTPRequestSender("http://"+config.getAddress()+"/api/put");
		this.templateSample = new DataSample(config.getMetric(), new Date().getTime(), config.getMax(), config.getTags());
	}

	public GeneratorWorker() {

	}

	public void run() {
		Gson gson = new Gson();

		Map<String, String> baseTags = new HashMap<String, String>(templateSample.getTags());

		for (int i = 0; i < config.getNumberOfRequests(); i++) {
			templateSample.setTimestamp(new Date().getTime());

			for (int j = 0; j < config.getDuplicate(); j++) {
				randomizeTemplete();
				templateSample.setTags(getNewTagsForSample(baseTags, j));
				String json = gson.toJson(templateSample);
				System.out.println(" Sending: " + json);
				int responseCode = sender.sendDataSample(json);
				System.out.println(" Response code: " + responseCode);
			}

			Thread.currentThread();
			try {
				Thread.sleep(config.getDelay());
			} catch (InterruptedException e) {
				message = "Error while performing sleep operation: "+e.getMessage();
				return;
			}
		}

		message = "Worker ended job without errors";
	}

	private Map<String, String> getNewTagsForSample(Map<String, String> baseTags, int j) {

		Map<String, String> resultMap = new HashMap<String, String>();

		for (Entry<String, String> entry : baseTags.entrySet()) {
			resultMap.put(entry.getKey(), entry.getValue() + j);
		}

		return resultMap;
	}

	private void randomizeTemplete() {
		templateSample.setValue(config.getMin() + (config.getMax() - config.getMin()) * random.nextDouble());
	}

	public String getResultMessage() {
		return message;
	}

}
