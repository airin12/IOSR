package pl.edu.agh.iosr.data;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class DataSampleTest {

	private DataSample sample = new DataSample();
	
	@Test
	public void constructorTest(){
		String metric = "metric";
		long timestamp = new Date().getTime();
		double value = Double.MIN_VALUE;
		Map<String,String> tags = new HashMap<>();
		
		sample = new DataSample(metric,timestamp,value,tags);
		
		assertEquals(metric,sample.getMetric());
		assertEquals(value, sample.getValue(),0.0);
		assertEquals(timestamp, sample.getTimestamp());
		assertEquals(tags, sample.getTags());
		
		
	}
	
	@Test
	public void testSetMetric(){
		String metric = "metric";
		sample.setMetric(metric);
		assertEquals(metric, sample.getMetric());
	}
	
	@Test
	public void testSetTimestamp(){
		long timestamp = new Date().getTime();
		sample.setTimestamp(timestamp);
		assertEquals(timestamp, sample.getTimestamp());
	}
	
	@Test
	public void testSetValue(){
		double value = Double.MAX_VALUE;
		sample.setValue(value);
		assertEquals(value, sample.getValue(),0.0);
	}
	
	@Test
	public void testSetTags(){
		Map<String,String> tags = new HashMap<>();
		sample.setTags(tags);
		assertEquals(tags, sample.getTags());
	}
	
}
