package pl.edu.agh.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
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
	
	public JavaRDD<Row> loadTSDBData(JavaPairRDD<Long,Long> rdd, final String combinedQuery, final String configFile){
		JavaRDD<Row> newRdd = rdd.flatMap(new FlatMapFunction<Tuple2<Long,Long>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Row> call(Tuple2<Long, Long> tuple) throws Exception {

				TSDBQueryParametrization queryParametrization = new TSDBQueryParametrizationBuilder().buildFromCombinedQuery(combinedQuery);
				Map<String,String> tags = queryParametrization.getTags();
				Config config = new Config(configFile);
					
				TSDB tsdb = new TSDB(config);
				Query query= tsdb.newQuery();
		        query.setTimeSeries(queryParametrization.getMetric(), queryParametrization.getTags(), queryParametrization.getAggregator(), false);
				query.setStartTime(tuple._1.longValue());
				query.setEndTime(tuple._2.longValue());
				DataPoints[] matchingPoints = query.run();
				DataPointsConverter parser = new DataPointsConverter();
				List<SingleRow> singleRows = parser.convertToSingleRows(matchingPoints, tags, tuple);
				List<Row> rows = new ArrayList<Row>();
				
				int size = queryParametrization.getTags().values().size() + 2;
				
				
				for(SingleRow singleRow : singleRows){
					Object [] arrays = new Object[size];
					arrays[0] = new Long(singleRow.getTimestamp());
					arrays[1] = new Double(singleRow.getValue());
					Iterator<String> it = queryParametrization.getTags().keySet().iterator();
					int index = 0;
					while(it.hasNext()){
						arrays[2+index] = singleRow.getTags().get(it.next());
						index++;
					}
					rows.add(RowFactory.create(arrays));
				}
				return rows;
			}
		});
		

		
		return newRdd;
	}
}
