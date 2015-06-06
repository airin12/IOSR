package pl.edu.agh.iosr.worker.test;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map.Entry;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.http.HTTPRequestSender;
import pl.edu.agh.iosr.worker.OpenTSDBWorker;

public class TestCaseOne implements OpenTSDBWorker{
	
	private Configuration config;
	private String message;

	public TestCaseOne(Configuration config) {
		this.config = config;
	}

	@Override
	public void run() {
		
		StringWriter addressWriter = new StringWriter();
		addressWriter.write("http://");
		addressWriter.write(config.getAddress());
		addressWriter.write("/grafana-rest-service/grafana/query/test/");
		addressWriter.write(String.valueOf(config.getStart())+"/");
		addressWriter.write(String.valueOf(config.getEnd())+"/");
		addressWriter.write(config.getMetric()+"/");
		addressWriter.write(config.getAggregator()+"/");
		
		Iterator<Entry<String,String>> it = config.getTags().entrySet().iterator();
		
		while(it.hasNext()){
			Entry<String, String> entry = it.next();
			addressWriter.write(entry.getKey()+"="+entry.getValue());
			if(it.hasNext())
				addressWriter.write(";");
		}
		
		HTTPRequestSender sender = new HTTPRequestSender(addressWriter.toString());
		
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
				String result = sender.sendTestCaseOneRequest();
				System.out.println("Requests performed in time: "+result+" ns");
				bw.write(result+";"+String.valueOf(config.getEnd()-config.getStart()));
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
