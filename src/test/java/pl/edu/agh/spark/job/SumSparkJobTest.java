package pl.edu.agh.spark.job;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.when;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import pl.edu.agh.spark.TSDBQueryParametrization;
import pl.edu.agh.spark.job.SumSparkJob;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TSDB.class)
public class SumSparkJobTest {
    private static final Double MIN_VALUE = 1.0;

    private SumSparkJob sumSparkJob;
    private TSDB mockedTSDB;
    @Mock
    private JavaSparkContext mockedSparkContext;
    @Mock
    private DataPoints mockedDataPoints;
    @Mock
    private DataPoint mockedDataPoint;
    @Mock
    private Query mockedQuery;
    @Mock
    private TSDBQueryParametrization mockedQueryParametrization;
    @Mock
    private JavaRDD<Double> mockedRdd;
    @Mock
    private SeekableView mockedIterator;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        // using PowerMockito, because final class (like TSDB) cannot be mocked with Mockito
        mockedTSDB = PowerMockito.mock(TSDB.class);
        when(mockedTSDB.newQuery()).thenReturn(mockedQuery);
        when(mockedQuery.run()).thenReturn(new DataPoints[]{mockedDataPoints});
        when(mockedDataPoints.iterator()).thenReturn(mockedIterator);
        when(mockedIterator.hasNext()).thenReturn(true).thenReturn(false);
        when(mockedIterator.next()).thenReturn(mockedDataPoint);
        when(mockedDataPoint.toDouble()).thenReturn(MIN_VALUE);
        when(mockedSparkContext.parallelize(anyList())).thenReturn(mockedRdd);
        when(mockedRdd.reduce(any(Function2.class))).thenReturn(MIN_VALUE);
        sumSparkJob = new SumSparkJob(mockedTSDB, mockedSparkContext);
    }

    @Test
    public void shouldReturnMinValueReturnedByRDD(){
        assertEquals(MIN_VALUE, sumSparkJob.execute(mockedQueryParametrization));
    }

}