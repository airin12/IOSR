package agh.edu.pl.rest;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.apache.spark.deploy.SparkSubmit;

@Path("/grafana")
public class GrafanaService {

	public static Map<String,Object> resultMap = new HashMap<String,Object>();
	
	@GET
	@Path("/query")
	public String executeSparkJob(){
		   
		SparkSubmit.main("--class agh.edu.pl.spark.SparkJobRunner  --deploy-mode client --master spark://172.17.84.76:7077 /root/files/spark.jar".split(" "));
		return resultMap.get("job").toString();
		
	}
	
}
