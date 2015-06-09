package pl.edu.agh.iosr.worker.test;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.data.GrafanaServiceRequest;
import pl.edu.agh.iosr.data.SingleQuery;
import pl.edu.agh.iosr.http.HTTPRequestSender;
import pl.edu.agh.iosr.worker.OpenTSDBWorker;

import com.google.gson.Gson;

public class TestCaseTwo implements OpenTSDBWorker{

	private Configuration config;
	private String message;

	public TestCaseTwo(Configuration config) {
		this.config = config;
	}

	@Override
	public void run() {
		
		SingleQuery query = new SingleQuery(config.getAggregator(), config.getMetric(), config.getSql(), config.getTags());
		
		List<SingleQuery> queries = new ArrayList<SingleQuery>();
		queries.add(query);		
		GrafanaServiceRequest request = new GrafanaServiceRequest(config.getStart(), config.getEnd(), queries);
		
		Gson gson = new Gson();
		String json = gson.toJson(request);
		
		HTTPRequestSender sender = new HTTPRequestSender("http://"+config.getGrafanaServiceAddress()+"/grafana-rest-service/grafana/query");
		
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(config.getFile());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			message = "Error while reading file "+config.getFile();
			return;
		}
	 
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
	 		
		try {
			for(int i = 0 ; i < config.getNumberOfRequests() ; i++){
				long start = System.nanoTime();
				String result = sender.sendTestCaseTwoRequest(json);
				long end = System.nanoTime();
				String resultTime = String.valueOf(end-start);
				
				System.out.println("Response in "+resultTime+" ns. Result: "+result);
				
				bw.write(resultTime+";"+String.valueOf(config.getEnd()-config.getStart()));
				bw.newLine();
				Thread.sleep(config.getDelay());
			}
		} catch (IOException e) {
			e.printStackTrace();
			message = "Error while writing to file "+config.getFile();
			return;
		} catch (InterruptedException e) {
			e.printStackTrace();
			message = "Error while performing sleep operaton";
			return;
		}
		
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
			message = "Error while closing file "+config.getFile();
			return;
		}
		
		message = "Succesfully performed requests";
	}

	@Override
	public String getResultMessage() {
		return message;
	}

}
