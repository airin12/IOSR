package pl.edu.agh.iosr.worker.test;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.http.HTTPRequestSender;
import pl.edu.agh.iosr.worker.OpenTSDBWorker;

public class TestCaseThreeMain implements OpenTSDBWorker{

	private Configuration config;
	private String message;
	private int actualCount = 0;
	private String addressString;

	public TestCaseThreeMain(Configuration config) {
		this.config = config;
		
		StringWriter addressWriter = new StringWriter();
		addressWriter.write("http://");
		addressWriter.write(config.getGrafanaServiceAddress());
		addressWriter.write("/grafana-rest-service/grafana/query/test/");
		addressWriter.write(String.valueOf(new Date().getTime())+"/");
		addressWriter.write("%s/");
		addressWriter.write(config.getMetric()+"/");
		addressWriter.write(config.getAggregator()+"/");
		
		Iterator<Entry<String,String>> it = config.getTags().entrySet().iterator();
		
		while(it.hasNext()){
			Entry<String, String> entry = it.next();
			addressWriter.write(entry.getKey()+"="+entry.getValue());
			if(it.hasNext())
				addressWriter.write(";");
		}
		
		addressString = addressWriter.toString();
	}

	@Override
	public void run() {		
		HTTPRequestSender sender = new HTTPRequestSender();
		
		OpenTSDBWorker worker = new TestCaseThreeHelperThread(config);
		Thread th = new Thread(worker);
		th.start();
		
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
			while(actualCount < config.getNumberOfRequests()){
				sender.setAddress(String.format(addressString, new Date().getTime()));
				String result = sender.sendTestCaseOneRequest();
				long currentTime = new Date().getTime();
				result = result.replace(")","").split(",")[1];
				actualCount = Integer.parseInt(result);
		
				System.out.println("Time: "+currentTime+" count: "+String.valueOf(actualCount));
				bw.write(String.valueOf(currentTime)+";"+String.valueOf(actualCount));
				bw.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
			message = "Error while writing to file "+config.getFile();
			return;
		}
		
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
			message = "Error while closing file "+config.getFile();
			return;
		}
		
		try {
			th.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		message = "Succesfully performed requests";
	}

	@Override
	public String getResultMessage() {
		return message;
	}

}
