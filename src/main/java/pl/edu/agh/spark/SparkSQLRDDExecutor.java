package pl.edu.agh.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import pl.edu.agh.model.SingleRow;
import pl.edu.agh.util.DataPointsConverter;
import scala.Tuple2;

public class SparkSQLRDDExecutor implements Serializable{
	
	private static final long serialVersionUID = -8063956721679295947L;

	public JavaRDD<Row> execute(JavaRDD<SingleRow> rdd){
		JavaRDD<Row> rowRDD = rdd.map(new Function<SingleRow, Row>() {
			private static final long serialVersionUID = 4965429980431362097L;

			public Row call(SingleRow record) throws Exception {
				return RowFactory.create(new Long(record.getTimestamp()),new Double(record.getValue()),record.getTags().get("cpu"));
			}
		});
		return rowRDD;
	}
	
	public JavaRDD<Row> loadTSDBData(JavaPairRDD<Long,Long> rdd, final String combinedQuery){
		JavaRDD<Row> newRdd = rdd.flatMap(new FlatMapFunction<Tuple2<Long,Long>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Row> call(Tuple2<Long, Long> tuple) throws Exception {

				TSDBQueryParametrization queryParametrization = new TSDBQueryParametrizationBuilder().buildFromCombinedQuery(combinedQuery);
				Map<String,String> tags = queryParametrization.getTags();
				Config config = new Config("/root/files/opentsdb.conf");
					
				TSDB tsdb = new TSDB(config);
				Query query= tsdb.newQuery();
		        query.setStartTime(queryParametrization.getStartTime());
		        query.setEndTime(queryParametrization.getEndTime());
		        query.setTimeSeries(queryParametrization.getMetric(), queryParametrization.getTags(), queryParametrization.getAggregator(), false);
				query.setStartTime(tuple._1.longValue());
				query.setEndTime(tuple._2.longValue());
				DataPoints[] matchingPoints = query.run();
				DataPointsConverter parser = new DataPointsConverter();
				List<SingleRow> singleRows = parser.convertToSingleRows(matchingPoints, tags);
				List<Row> rows = new ArrayList<Row>();
				
				for(SingleRow singleRow : singleRows)
					rows.add(RowFactory.create(new Long(singleRow.getTimestamp()),new Double(singleRow.getValue()),singleRow.getTags().get("cpu")));
				
				return rows;
			}
		});
		

		
		return newRdd;
	}
}