package agh.edu.pl.util;

import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowConverterTest {
    private static final String EXPECTED_JSON = "[{\"metric\":\"metric\",\"tags\":{\"tag\":\"tag\"},\"aggregateTags\":[],\"dps\":{\"5\":10.0}}]";
    private static final String EXPECTED_JSON_WITH_TWO_ROWS = "[{\"metric\":\"metric\",\"tags\":{\"tag\":\"tag\"},\"aggregateTags\":[],\"dps\":{\"5\":10.0}},{\"metric\":\"metric\",\"tags\":{\"tag\":\"tag2\"},\"aggregateTags\":[],\"dps\":{\"3\":99.0}}]";
    private static final String TAG = "tag";
    private static final String METRIC = "metric";
    private RowConverter rowConverter;
    private Row mockedRow;
    private Row mockedRow2;

    @Before
    public void setUp() throws Exception {
        rowConverter = new RowConverter();
        mockedRow = mock(Row.class);
        when(mockedRow.getLong(0)).thenReturn(5L);
        when(mockedRow.getDouble(1)).thenReturn(10.0);
        when(mockedRow.getString(2)).thenReturn("tag");
        mockedRow2 = mock(Row.class);
        when(mockedRow2.getLong(0)).thenReturn(3L);
        when(mockedRow2.getDouble(1)).thenReturn(99.0);
        when(mockedRow2.getString(2)).thenReturn("tag2");
    }

    @Test
    public void shouldConvert(){
        String result = rowConverter.convertToJSONString(Arrays.asList(mockedRow), Arrays.asList(TAG), METRIC);
        assertEquals(EXPECTED_JSON, result);
    }

    @Test
    public void shouldConvertTwoRows(){
        String result = rowConverter.convertToJSONString(Arrays.asList(mockedRow, mockedRow2), Arrays.asList(TAG), METRIC);
        assertEquals(EXPECTED_JSON_WITH_TWO_ROWS, result);
    }
}