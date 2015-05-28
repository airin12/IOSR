package pl.edu.agh.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import pl.edu.agh.model.SingleRow;
import net.opentsdb.core.DataPoints;

public class DataPointsConverter {

	public List<SingleRow> convertToSingleRows(DataPoints[] datapoints, Map<String,String> tags){
		List<SingleRow> resultList = new ArrayList<SingleRow>();
		
		for(DataPoints points : datapoints){
			for(int i = 0;i<points.aggregatedSize();i++){
				SingleRow row = new SingleRow();
				row.setTags(tags);
				row.setTimestamp(points.timestamp(i));
				row.setValue(points.doubleValue(i));
				
				resultList.add(row);
			}
		}
		
		return resultList;
	}
}
