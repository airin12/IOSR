package agh.edu.pl.spark;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import agh.edu.pl.model.SingleRow;

public class SparkSQLRDDExecutor implements Serializable{
	
	private static final long serialVersionUID = -8063956721679295947L;

	JavaRDD<Row> execute(JavaRDD<SingleRow> rdd){
		JavaRDD<Row> rowRDD = rdd.map(new Function<SingleRow, Row>() {
			private static final long serialVersionUID = 4965429980431362097L;

			public Row call(SingleRow record) throws Exception {
				return RowFactory.create(new Long(record.getTimestamp()),new Double(record.getValue()),record.getTags().get("cpu"));
			}
		});
		return rowRDD;
	}
}
