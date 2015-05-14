package agh.edu.pl.rest;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.deploy.SparkSubmit;

@Path("/grafana")
public class GrafanaService {

	public static final Logger LOGGER = LogManager.getLogger(GrafanaService.class);
	public static Map<String, Object> resultMap = new HashMap<String, Object>();

	@GET
	@Path("/query/{start}/{end}/{metric}/{aggregator}/{tags}")
	public String executeSparkJob(@PathParam("start") String start, @PathParam("end") String end, @PathParam("metric") String metric,
			@PathParam("aggregator") String aggregator, @PathParam("tags") String tags) {

		String combinedQuery = createCombinedQuery(start,end,metric,aggregator,tags);
		if(combinedQuery == null)
			return "Bad query";
		
		SparkSubmit.main(("--class agh.edu.pl.spark.SparkJobRunner  --deploy-mode client --master spark://172.17.84.76:7077 /root/files/spark.jar function "+combinedQuery).split(" "));
		return resultMap.get("job").toString();

	}
	
	@GET
	@Path("/query/sql/{start}/{end}/{metric}/{aggregator}/{tags}/{sql}")
	public String executeSparkJobWithSql(@PathParam("start") String start, @PathParam("end") String end, @PathParam("metric") String metric,
			@PathParam("aggregator") String aggregator, @PathParam("tags") String tags, @PathParam("sql") String sql) {

		String combinedQuery = createCombinedQuery(start,end,metric,aggregator,tags);
		if(combinedQuery == null)
			return "Bad query";
		
		SparkSubmit.main(("--class agh.edu.pl.spark.SparkJobRunner  --deploy-mode client --master spark://172.17.84.76:7077 /root/files/spark.jar function "+combinedQuery).split(" "));
		return resultMap.get("job").toString();

	}
	
	@POST
	@Path("/query")
	@Consumes(MediaType.APPLICATION_JSON)
	public String executeSparkJob(String json) {
		SparkSubmit.main(("--class agh.edu.pl.spark.SparkJobRunner  --deploy-mode client --master spark://172.17.84.76:7077 /root/files/spark.jar json "+json).split(" "));
		return resultMap.get("job").toString();
	}

	private String createCombinedQuery(String start, String end, String metric, String aggregator, String tags) {
		if(start == null || start.equals(""))
			return null;
		else if( end == null || end.equals(""))
			return null;
		else if (metric == null || metric.equals(""))
			return null;
		else if( aggregator == null || aggregator.equals(""))
			return null;
		else if( tags == null || tags.equals(""))
			return null;
		
		return start+":"+end+":"+metric+":"+aggregator+":"+tags;
	}

}
