package pl.edu.agh.rest;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.deploy.SparkSubmit;

import pl.edu.agh.spark.SparkContextFactory;
import pl.edu.agh.spark.SparkJobRunnerModes;

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
		
		String [] args = {"--class",
						  "pl.edu.agh.spark.SparkJobRunner",
						  "--deploy-mode",
						  "client",
						  "",
						  SparkJobRunnerModes.BASIC.toString(),
						  combinedQuery};
		
		SparkSubmit.main(args);
		return resultMap.get("job").toString();

	}
	
	@GET
	@Path("/query/sql/{start}/{end}/{metric}/{aggregator}/{tags}/{sql}")
	public String executeSparkJobWithSql(@PathParam("start") String start, @PathParam("end") String end, @PathParam("metric") String metric,
			@PathParam("aggregator") String aggregator, @PathParam("tags") String tags, @PathParam("sql") String sql) {

		String combinedQuery = createCombinedQuery(start,end,metric,aggregator,tags);
		if(combinedQuery == null)
			return "Bad query";
		
		String [] args = {"--class",
				  "pl.edu.agh.spark.SparkJobRunner",
				  "--deploy-mode",
				  "client",
				  "",
				  SparkJobRunnerModes.BASIC.toString(),
				  combinedQuery};

		SparkSubmit.main(args);
		return resultMap.get("job").toString();

	}
	
	@GET
	@Path("/query/test/{start}/{end}/{metric}/{aggregator}/{tags}")
	public String executeTestJob(@PathParam("start") String start, @PathParam("end") String end, @PathParam("metric") String metric,
			@PathParam("aggregator") String aggregator, @PathParam("tags") String tags) {

		String combinedQuery = createCombinedQuery(start,end,metric,aggregator,tags);
		if(combinedQuery == null)
			return "Bad query";
		
		String [] args = {"--class",
				  "pl.edu.agh.spark.SparkJobRunner",
				  "--deploy-mode",
				  "client",
				  "",
				  SparkJobRunnerModes.TEST.toString(),
				  combinedQuery};

		SparkSubmit.main(args);
		return resultMap.get("job").toString();

	}
	
	@POST
	@Path("/query")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response executeSparkJob(String json, @HeaderParam("Access-Control-Request-Headers") String requestHeader) {
		ResponseBuilder builder = submitSparkJob(json);
		builder.header("Access-Control-Allow-Origin", "*")
			   .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
		
		if(requestHeader != null && requestHeader.length() > 0)
			builder.header("Access-Control-Allow-Headers", requestHeader);

		return builder.build();
	}
	
	@OPTIONS
	@Path("/query")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response executeSparkJobWithOptions(String json, @HeaderParam("Access-Control-Request-Headers") String requestHeader) {
		ResponseBuilder builder = Response.ok();
		builder.header("Access-Control-Allow-Origin", "*")
			   .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			   .header("Access-Control-Allow-Headers", requestHeader);

		return builder.build();
	}
	
	@GET
	@Path("/spark/close")
	public Response closeSparkContext(){
		new SparkContextFactory().closeSparkContext();
		
		return Response.ok().build();
	}
	
	private ResponseBuilder submitSparkJob(String json){
		
		String [] args = {"--class",
				  "pl.edu.agh.spark.SparkJobRunner",
				  "--deploy-mode",
				  "client",
				  "",
				  SparkJobRunnerModes.SQL.toString(),
				  json};

		SparkSubmit.main(args);
		return Response.ok().entity(resultMap.get("job").toString());
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
