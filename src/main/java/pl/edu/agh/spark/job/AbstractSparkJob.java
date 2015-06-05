package pl.edu.agh.spark.job;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pl.edu.agh.spark.TSDBQueryParametrization;
import scala.Tuple2;


public abstract class AbstractSparkJob implements SparkJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinSparkJob.class);
    private final TSDB tsdb;
    protected final JavaSparkContext sparkContext;

    public AbstractSparkJob(final TSDB tsdb, final JavaSparkContext sparkContext){
        this.tsdb = tsdb;
        this.sparkContext = sparkContext;
    }

    public Object execute(TSDBQueryParametrization queryParametrization){
        Query query = buildQuery(queryParametrization);
        DataPoints[] matchingPoints = query.run();
        if (matchingPoints.length == 0){
            LOGGER.info("No matching points for query found. Returning 0.0");
            return 0.0;
        }
        LOGGER.info("Fetched {} points.", matchingPoints[0].aggregatedSize());
        List<Double> values = extractValues(matchingPoints[0]);
        return execute(sparkContext.parallelize(values));
    }

    protected Query buildQuery(TSDBQueryParametrization queryParametrization) {
        Query query= tsdb.newQuery();
        query.setStartTime(queryParametrization.getStartTime());
        query.setEndTime(queryParametrization.getEndTime());
        query.setTimeSeries(queryParametrization.getMetric(), queryParametrization.getTags(), queryParametrization.getAggregator(), false);
        return query;
    }

    private List<Double> extractValues(DataPoints matchingPoint) {
        SeekableView iterator = matchingPoint.iterator();
        List<Double> values = new LinkedList<Double>();
        while(iterator.hasNext()){
            values.add(iterator.next().toDouble());
        }
        return values;
    }

    protected abstract Object execute(JavaRDD<Double> rdd);
    
    protected List<Tuple2<Long, Long>> generateTimestampsList(long startTime, long endTime, int slices) {
		List<Tuple2<Long, Long>> timestamps = new ArrayList<Tuple2<Long, Long>>();

		long diff = (endTime - startTime) / slices;
		long actualTimestamp = startTime;

		while (actualTimestamp < endTime) {
			Long start = new Long(actualTimestamp);
			Long end;

			if (actualTimestamp + diff > endTime)
				end = new Long(endTime);
			else
				end = new Long(actualTimestamp + diff);

			actualTimestamp += diff + 1;

			timestamps.add(new Tuple2<Long, Long>(start, end));
		}

		return timestamps;
	}

	protected List<String> generateTagsListFromMap(Map<String, String> map) {
		Object[] tagsArray = map.keySet().toArray();
		List<String> tagsList = new ArrayList<String>();

		for (Object tag : tagsArray)
			tagsList.add(tag.toString());

		return tagsList;
	}
}
