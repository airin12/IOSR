package pl.edu.agh.spark;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;

import org.junit.Before;
import org.junit.Test;

import pl.edu.agh.spark.TSDBQueryParametrization;
import pl.edu.agh.spark.TSDBQueryParametrizationBuilder;
import static org.junit.Assert.*;

public class TSDBQueryParametrizationBuilderTest {
    private static final String COMBINED_QUERY = "100:200:metric:sum:key=value";
    private static final String JSON = "{\"start\":100,\"end\":200, \"queries\":[{\"sql\":\"sql\", \"aggregator\":\"sum\",\"metric\":\"metric\",\"tags\":{\"cpu\":\"01\"}}]}";
    private static final Long EXPECTED_START_TIME = 100l;
    private static final Long EXPECTED_END_TIME = 200L;
    private static final String EXPECTED_METRIC = "metric";
    private static final Aggregator EXPECTED_AGGREGATOR = Aggregators.SUM;

    private TSDBQueryParametrizationBuilder builder;

    @Before
    public void setUp() throws Exception {
        builder = new TSDBQueryParametrizationBuilder();
    }

    @Test
    public void shouldBuildStartTimeFromJson(){
        TSDBQueryParametrization parametrization = builder.buildFromJson(JSON);
        assertTrue(EXPECTED_START_TIME == parametrization.getStartTime());
    }

    @Test
    public void shouldBuildEndTimeFromJson(){
        TSDBQueryParametrization parametrization = builder.buildFromJson(JSON);
        assertTrue(EXPECTED_END_TIME == parametrization.getEndTime());
    }

    @Test
    public void shouldBuildMetricFromJson(){
        TSDBQueryParametrization parametrization = builder.buildFromJson(JSON);
        assertEquals(EXPECTED_METRIC, parametrization.getMetric());
    }

    @Test
    public void shouldBuildAggregatorFromJson(){
        TSDBQueryParametrization parametrization = builder.buildFromJson(JSON);
        assertEquals(EXPECTED_AGGREGATOR, parametrization.getAggregator());
    }

    @Test
    public void shouldBuildStartTimeFromCombinedQuery(){
        TSDBQueryParametrization parametrization = builder.buildFromCombinedQuery(COMBINED_QUERY);
        assertTrue(EXPECTED_START_TIME == parametrization.getStartTime());
    }

    @Test
    public void shouldBuildEndTimeFromCombinedQuery(){
        TSDBQueryParametrization parametrization = builder.buildFromCombinedQuery(COMBINED_QUERY);
        assertTrue(EXPECTED_END_TIME == parametrization.getEndTime());
    }

    @Test
    public void shouldBuildMetricFromCombinedQuery(){
        TSDBQueryParametrization parametrization = builder.buildFromCombinedQuery(COMBINED_QUERY);
        assertEquals(EXPECTED_METRIC, parametrization.getMetric());
    }

    @Test
    public void shouldBuildAggregatorFromCombinedQuery(){
        TSDBQueryParametrization parametrization = builder.buildFromCombinedQuery(COMBINED_QUERY);
        assertEquals(EXPECTED_AGGREGATOR, parametrization.getAggregator());
    }

    @Test
    public void shouldBuildTagsFromCombinedQuery(){
        TSDBQueryParametrization parametrization = builder.buildFromCombinedQuery(COMBINED_QUERY);
        assertEquals(1, parametrization.getTags().size());
        assertEquals("value", parametrization.getTags().get("key"));
    }
}