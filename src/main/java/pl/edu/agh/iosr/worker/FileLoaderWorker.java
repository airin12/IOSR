package pl.edu.agh.iosr.worker;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import pl.edu.agh.iosr.config.Configuration;
import pl.edu.agh.iosr.data.DataSample;
import pl.edu.agh.iosr.http.HTTPRequestSender;

import com.google.gson.Gson;

public class FileLoaderWorker implements OpenTSDBWorker{

	private Configuration config;
	private HTTPRequestSender sender;
	private String message;
	private Gson gson = new Gson();
	
	
	public FileLoaderWorker(Configuration config) {
		this.config = config;
		sender = new HTTPRequestSender("http://"+config.getAddress()+"/api/put");
	}
	
	public void run(){
		FileInputStream fstream;
		try {
			fstream = new FileInputStream(config.getFile());
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			
			String strLine;

			while ((strLine = br.readLine()) != null)   {
			  	processLine(strLine);
			}

			br.close();
			
			message = "Worker ended job without errors";
			
		} catch (FileNotFoundException e) {
			message = "File "+config.getFile()+" does not exist";
		} catch (IOException e) {
			message = "Error while reading file";
		} catch (Exception e) {
			message = "Error while reading lines from file: "+e.getMessage();
		}
		
		
	}

	private void processLine(String strLine) throws Exception{
		String [] columns = strLine.split(config.getSeparator());
		Map<String,Integer> columnMap = config.getColumnMap();
		Map<String,String> tags = new HashMap<String,String>();
		tags.putAll(config.getTags());
		
		DataSample sample = new DataSample();
		sample.setTimestamp(new Date().getTime());
		
		for(Entry<String,Integer> entry : columnMap.entrySet()){
			if(entry.getKey().equals(Configuration.TIMESTAMP_COL)){
				sample.setTimestamp(Long.parseLong(columns[entry.getValue().intValue()]));
			} else if(entry.getKey().equals(Configuration.VALUE_COL)){
				sample.setValue(Double.parseDouble(columns[entry.getValue().intValue()]));
			} else {
				tags.put(entry.getKey(), columns[entry.getValue().intValue()]);
			}
		}
		
		sample.setMetric(config.getMetric());
		sample.setTags(tags);
		
		if(tags.size() == 0 ){
			throw new IllegalArgumentException(" You must specify at least one tag");
		}
		
		String json = gson.toJson(sample);
		System.out.println("Sending: "+ json);
		int response = sender.sendDataSample(json);
		System.out.println("Response: "+response);
		
		Thread.sleep(config.getDelay());
		
	}

	public String getResultMessage() {
		return message;
	}

}
