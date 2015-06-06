package pl.edu.agh.iosr.http;

import static org.junit.Assert.*;

import org.junit.Test;

public class HTTPRequestSenderTest {

	private String json = "{\"metric\":\"mem.usage.perc\",\"timestamp\":1433604925985,\"value\":84.61483654647658,\"tags\":{\"cpu\":\"01\"}}";
	
	@Test
	public void testSetAddress(){
		String address = "10.1.1.1";
		HTTPRequestSender sender = new HTTPRequestSender();
		sender.setAddress(address);
		assertEquals(address,sender.getAddress());
	}
	
	@Test
	public void testSendJson(){
		String address = "http://172.17.84.76:2002/api/put";
		HTTPRequestSender sender = new HTTPRequestSender(address);
		int result = sender.sendDataSample(json);
		assertEquals(204, result);
	}
	
	@Test
	public void testSendJsonMalformedURL(){
		String address = "htasf://172.17.121.76:2002/api/put";
		HTTPRequestSender sender = new HTTPRequestSender(address);
		int result = sender.sendDataSample(json);
		assertEquals(-1, result);
	}
	
	@Test
	public void testSendTestRequest(){
		String address = "http://172.17.84.76:7001/grafana-rest-service/grafana/query/test/1429833600/1430006400/mem.usage.perc/sum/cpu=00";
		HTTPRequestSender sender = new HTTPRequestSender(address);
		String result = sender.sendTestCaseOneRequest();
		assertNotNull(result);
	}
	
	@Test
	public void testSendTestRequestMalformedURL(){
		String address = "hasad://172.17.84.76:7001/grafana-rest-service/grafana/query/test/1429833600/1430006400/mem.usage.perc/sum/cpu=00";
		HTTPRequestSender sender = new HTTPRequestSender(address);
		String result = sender.sendTestCaseOneRequest();
		assertNull(result);
	}
}
