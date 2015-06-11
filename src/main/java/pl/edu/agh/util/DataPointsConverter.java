package pl.edu.agh.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.DataPoints;
import pl.edu.agh.model.SingleRow;
import scala.Tuple2;

public class DataPointsConverter {

	public List<SingleRow> convertToSingleRows(DataPoints[] datapoints, Map<String,String> tags, Tuple2<Long,Long> timestamps){
		List<SingleRow> resultList = new ArrayList<SingleRow>();
		
		long start = timestamps._1;
		long end = timestamps._2;
		
		if(String.valueOf(start).length() == 13)
			start /= 1000;
		
		if(String.valueOf(end).length() == 13)
			end /= 1000;
		
		for(DataPoints points : datapoints){
			for(int i = 0;i<points.aggregatedSize();i++){
				long timestamp = points.timestamp(i);
				if(String.valueOf(timestamp).length() == 13)
					timestamp /= 1000;
				
				if(timestamp >= start && timestamp <= end){
					SingleRow row = new SingleRow();
					row.setTags(points.getTags());
					row.setTimestamp(timestamp);
					row.setValue(points.doubleValue(i));
					
					resultList.add(row);
				}
			}
		}
		
		return resultList;
	}
}
