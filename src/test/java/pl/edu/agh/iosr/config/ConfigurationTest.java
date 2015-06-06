package pl.edu.agh.iosr.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

public class ConfigurationTest {

	private final String noModeMessage = "You must specify mode parameter";
	private final String noMetricMessage = "You must specify metric parameter";
	private final String noAddressMessage = "You must specify address parameter";
	private final String noTagsMessage = "You must specify tags parameter";
	private final String noRequestsMessage = "You must specify req_nr parameter";
	private final String noFileMessage = "You must specify file parameter";
	private final String noSeparatorMessage = "You must specify separator parameter";
	private final String noFormatMessage = "You must specify format parameter";
	private final String noAggregatorMessage = "You must specify aggregator parameter";
	
	@Test
	public void testNoModeSet(){
		String [] args = new String[]{""};
		Configuration config = new Configuration(args);
		assertEquals(false,config.isValid());
		assertEquals(noModeMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoMetricSet(){
		String [] args = new String[]{"mode=LOAD"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noMetricMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoAddressSet(){
		String [] args = new String[]{"mode=LOAD","metric=agh"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noAddressMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoTagsSetInGenerateMode(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noTagsMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoRequestsSetInGenerateMode(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
									  "tags=cpu:01"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noRequestsMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoFileSetInLoadMode(){
		String [] args = new String[]{"mode=LOAD","metric=agh","address=localhost:2002",
									  "tags=cpu:01"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noFileMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoSeparatorSetInLoadMode(){
		String [] args = new String[]{"mode=LOAD","metric=agh","address=localhost:2002",
									  "tags=cpu:01","file=data.txt"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noSeparatorMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoFormatSetInLoadMode(){
		String [] args = new String[]{"mode=LOAD","metric=agh","address=localhost:2002",
									  "tags=cpu:01","file=data.txt","separator=:"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noFormatMessage, config.getErrorMsg());
	}
	
	@Test
	public void testNoAggregatorSetInLoadMode(){
		String [] args = new String[]{"mode=TEST1","metric=agh","address=localhost:2002",
									  "tags=cpu:01","req_nr=100","file=data.txt"};
		Configuration config = new Configuration(args);
		assertEquals(false, config.isValid());
		assertEquals(noAggregatorMessage, config.getErrorMsg());
	}
	
	@Test
	public void testSetNrOfRequests(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(100, config.getNumberOfRequests());
	}
	
	@Test
	public void testSetDelay(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","delay=100"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(100, config.getDelay());
	}
	
	@Test
	public void testSetMax(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","max=100"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(100, config.getMax(),0.0);
	}
	
	@Test
	public void testSetMin(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","min=100"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(100, config.getMin(),0.0);
	}
	
	@Test
	public void testSetStart(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","start=100"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(100, config.getStart(),0.0);
	}
	
	@Test
	public void testSetEnd(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","end=100"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(100, config.getEnd(),0.0);
	}
	
	@Test
	public void testSetAgr(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","aggregator=sum"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals("sum",config.getAggregator());
	}
	
	@Test
	public void testSetStep(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","step=2"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(2,config.getTimeStep());
	}
	
	@Test
	public void testSetFile(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","step=2","file=data.txt"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals("data.txt",config.getFile());
	}
	
	@Test
	public void testSetDup(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","duplicate=2"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(2,config.getDuplicate());
	}
	
	@Test
	public void testSetSeparator(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","separator=:"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals(":",config.getSeparator());
	}
	
	@Test
	public void testSetFormat(){
		String [] args = new String[]{"mode=GENERATE","metric=agh","address=localhost:2002",
				  "tags=cpu:01","req_nr=100","format=timestamp:value:","separator=:"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		assertEquals("timestamp:value:",config.getFormat());
	}
	
	@Test
	public void testSettingFormatData(){
		String [] args = new String[]{"mode=LOAD","metric=agh","address=localhost:2002",
				  "file=data.txt","separator=:","format=timestamp:value:cpu"};
		Configuration config = new Configuration(args);
		assertTrue(config.isValid());
		Map<String,Integer> columnMap = config.getColumnMap();
		assertEquals(new Integer(0), columnMap.get("timestamp"));
		assertEquals(new Integer(1), columnMap.get("value"));
		assertEquals(new Integer(2), columnMap.get("cpu"));
		List<String> columnNames = config.getTagsNamesFromFile();
		assertEquals("cpu", columnNames.get(0));
		assertEquals(1, columnNames.size());
	}
	
}
