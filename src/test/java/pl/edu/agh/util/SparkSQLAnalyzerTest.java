package pl.edu.agh.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SparkSQLAnalyzerTest {

    private static final String SELECT_WITH_ASTERISK = "select * from rows";
    private static final String SELECT_FOR_VALUE_COLUMN = "select value from rows";
    private static final String SELECT_FOR_TIMESTAMP_COLUMN = "select timestamp from rows";
    private static final String SELECT_FOR_TIMESTAMP_AND_VALUE = "select timestamp, value from rows";
    private static final String SELECT_FOR_TAG_COLUMN = "select tag1 from rows";
    private static final String SELECT_FOR_NOT_TAG_COLUMN = "select nottag from rows";
    private static final String SELECT_FOR_THREE_TAGS = "select tag2, tag3, tag1 from rows";
    private static final String SELECT_FOR_TIMESTAMP_AND_TAG = "select timestamp, tag1 from rows";
    private static final List<String> TAGS_LIST = Arrays.asList("tag1", "tag2", "tag3");
    private static final String SELECT_FOR_VALUE_AND_TAG = "select tag1, value from rows";

    @Test
    public void shouldBeValidatedAsProperSqlWhenSelectingWithAsterisk(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_WITH_ASTERISK, TAGS_LIST).analyze();
        assertTrue(analyzer.isProperSql());
    }

    @Test
    public void shouldBeValidatedAsProperSqlWhenSelectingFromValueColumn(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_VALUE_COLUMN, TAGS_LIST).analyze();
        assertTrue(analyzer.isProperSql());
    }

    @Test
    public void shouldBeValidatedAsProperSqlWhenSelectingFromTimestampColumn(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_TIMESTAMP_COLUMN, TAGS_LIST).analyze();
        assertTrue(analyzer.isProperSql());
    }

    @Test
    public void shouldBeValidatedAsProperSqlWhenSelectingColumnFromTagList(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_TAG_COLUMN, TAGS_LIST).analyze();
        assertTrue(analyzer.isProperSql());
    }

    @Test
    public void shouldNotBeValidatedAsProperSql(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_NOT_TAG_COLUMN, TAGS_LIST).analyze();
        assertFalse(analyzer.isProperSql());
    }

    @Test
    public void shouldSetGraphResultTypeWhenColumnContainsAsteriks(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_WITH_ASTERISK, TAGS_LIST).analyze();
        assertEquals(SparkSQLAnalyzer.ResultTypes.GRAPH, analyzer.getResultType());
    }

    @Test
    public void shouldSetGraphResultTypeWhenColumnsContainValueAndTimestamp(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_TIMESTAMP_AND_VALUE, TAGS_LIST).analyze();
        assertEquals(SparkSQLAnalyzer.ResultTypes.GRAPH, analyzer.getResultType());
    }

    @Test
    public void shouldSetDataResultType(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_TAG_COLUMN, TAGS_LIST).analyze();
        assertEquals(SparkSQLAnalyzer.ResultTypes.DATA, analyzer.getResultType());
    }

    @Test
    public void shouldFillResultIndexesMap(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_THREE_TAGS, TAGS_LIST).analyze();
        Map<String, Integer> resultIndexesMap = analyzer.getResultIndexesMap();
        assertEquals(resultIndexesMap.get("tag1"), new Integer(2));
        assertEquals(resultIndexesMap.get("tag2"), new Integer(0));
        assertEquals(resultIndexesMap.get("tag3"), new Integer(1));
    }

    @Test
    public void shouldPutLongAsResultClassForTimestamp(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_TIMESTAMP_AND_TAG, TAGS_LIST).analyze();
        assertEquals(Long.class.toString(), analyzer.getResultClassesMap().get("timestamp"));
    }

    @Test
    public void shouldPutStringAsResultClassForTag(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_TIMESTAMP_AND_TAG, TAGS_LIST).analyze();
        assertEquals(String.class.toString(), analyzer.getResultClassesMap().get("tag1"));
    }

    @Test
    public void shouldPutDoubleAsResultClassForValue(){
        SparkSQLAnalyzer analyzer = new SparkSQLAnalyzer(SELECT_FOR_VALUE_AND_TAG, TAGS_LIST).analyze();
        assertEquals(Double.class.toString(), analyzer.getResultClassesMap().get("value"));
    }
}
