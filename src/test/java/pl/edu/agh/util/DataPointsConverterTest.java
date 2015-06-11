package pl.edu.agh.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.DataPoints;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import pl.edu.agh.model.SingleRow;
import pl.edu.agh.util.DataPointsConverter;
import scala.Tuple2;

@RunWith(MockitoJUnitRunner.class)
public class DataPointsConverterTest {

    private DataPointsConverter converter;
    private Map<String, String> tags;
    @Mock private DataPoints mockedDataPoints;

    @Before
    public void setUp(){
        converter = new DataPointsConverter();
        tags = new HashMap<>();
    }

    @Test
    public void shouldReturnEmptyListWhenDataPointsArrayIsEmpty(){
        DataPoints[] dataPoints = new DataPoints[0];
        List<SingleRow> result = converter.convertToSingleRows(dataPoints, tags, new Tuple2<Long, Long>(0L, 1L));
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void shouldReturnListWithTwoElements(){
        DataPoints[] dataPoints = new DataPoints[1];
        dataPoints[0] = prepareMockedDataPoints(2);
        when(mockedDataPoints.aggregatedSize()).thenReturn(2);

        List<SingleRow> result = converter.convertToSingleRows(dataPoints, tags, new Tuple2<Long, Long>(0L, 1L));

        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnListWithSixElements(){
        DataPoints[] dataPoints = new DataPoints[3];
        dataPoints[0] = prepareMockedDataPoints(1);
        dataPoints[1] = prepareMockedDataPoints(2);
        dataPoints[2] = prepareMockedDataPoints(3);

        List<SingleRow> result = converter.convertToSingleRows(dataPoints, tags, new Tuple2<Long, Long>(0L, 1L));

        assertEquals(6, result.size());
    }

    private DataPoints prepareMockedDataPoints(int numberOfElements){
        DataPoints mockedDataPoints = mock(DataPoints.class);
        when(mockedDataPoints.aggregatedSize()).thenReturn(numberOfElements);
        for (int i = 0; i < numberOfElements; i++){
            when(mockedDataPoints.timestamp(i)).thenReturn(i * 3L);
            when(mockedDataPoints.doubleValue(i)).thenReturn(i * 3.0);
        }

        return mockedDataPoints;
    }
}