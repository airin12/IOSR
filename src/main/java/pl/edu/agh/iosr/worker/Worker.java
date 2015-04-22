package pl.edu.agh.iosr.worker;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import pl.edu.agh.iosr.data.DataSample;
import pl.edu.agh.iosr.http.HTTPRequestSender;

import com.google.gson.Gson;

public class Worker implements Runnable{

	private int iterations;
	private long delay;
	private double minValue;
	private double maxValue;
	private DataSample templateSample;
	private final Random random = new Random(new Date().getTime());	
	private HTTPRequestSender sender;
	private int samplesInTime; 

	public Worker(int iterations, long delay, double minValue, double maxValue,
			DataSample templateSample, HTTPRequestSender sender,
			int samplesInTime) {
		super();
		this.iterations = iterations;
		this.delay = delay;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.templateSample = templateSample;
		this.sender = sender;
		this.samplesInTime = samplesInTime;
	}


	public Worker(){
		
	}


	public void run() {
		Gson gson = new Gson();
		
		Map<String,String> baseTags = new HashMap<String,String>(templateSample.getTags());
		
		for(int i=0;i<iterations;i++){
			templateSample.setTimestamp(new Date().getTime());
			
			for(int j=0;j<samplesInTime;j++){
				randomizeTemplete();
				templateSample.setTags(getNewTagsForSample(baseTags,j));
				String json = gson.toJson(templateSample);
				System.out.println(" Sending: "+ json);
				int responseCode = sender.sendDataSample(json);
				System.out.println(" Response code: "+responseCode);
			}
			
			
			Thread.currentThread();
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}
		
	}



	private Map<String, String> getNewTagsForSample(
			Map<String, String> baseTags, int j) {
		
		Map<String,String> resultMap = new HashMap<String,String>();
		
		for(Entry<String,String> entry : baseTags.entrySet()){
			resultMap.put(entry.getKey(), entry.getValue()+j);
		}
		
		return resultMap;
	}


	private void randomizeTemplete() {
		templateSample.setValue(minValue + (maxValue-minValue)*random.nextDouble());
	}

}
