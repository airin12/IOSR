package pl.edu.agh.iosr.config;

import static org.junit.Assert.assertEquals;

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
}
