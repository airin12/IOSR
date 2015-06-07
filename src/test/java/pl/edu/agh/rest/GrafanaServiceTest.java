package pl.edu.agh.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import org.apache.spark.deploy.SparkSubmit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import pl.edu.agh.rest.GrafanaService;

import javax.ws.rs.core.Response;
import java.util.Random;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SparkSubmit.class)
public class GrafanaServiceTest {

    private static final String JOB_START = "10";
    private static final String JOB_END = "20";
    private static final String METRIC = "CPU";
    private static final String AGGREGATOR = "SUM";
    private static final String TAGS = "TAGS";
    private static final String JSON = "JSON";
    private static final String REQUEST_HEADER = "RQ_HEADER";
    private static final String[] EXPECTED_ARGUMENTS = new String[]{"--class", "pl.edu.agh.spark.SparkJobRunner", "--deploy-mode", "client",
    		"", "BASIC", "10:20:CPU:SUM:TAGS", "1"};
    private static final String[] EXPECTED_ARGUMENTS_JSON = new String[]{"--class", "pl.edu.agh.spark.SparkJobRunner", "--deploy-mode", "client",
            "", "SQL", "JSON", "1"};
    private GrafanaService grafanaService;
    private Random random;


    @Before
    public void setUp(){
        grafanaService = new GrafanaService();
        random = mock(Random.class);
        grafanaService.setRandom(random);
        when(random.nextInt()).thenReturn(1);
        GrafanaService.resultMap.put("1", new Object());
        mockStatic(SparkSubmit.class);
        doNothing().when(SparkSubmit.class);
        SparkSubmit.main(any(String[].class));
    }

    @Test
    public void shouldCallSparkSubmitMainOnSparkJobExecution(){
        grafanaService.executeSparkJob(JOB_START, JOB_END, METRIC, AGGREGATOR, TAGS);
        verifyStatic();
        SparkSubmit.main(EXPECTED_ARGUMENTS);
    }

    @Test
    public void shoudCallSparkSumbitMainWithJson(){
        grafanaService.executeSparkJob(JSON, REQUEST_HEADER);
        verifyStatic();
        SparkSubmit.main(EXPECTED_ARGUMENTS_JSON);
    }

    @Test
    public void shouldCallSparkSubmitMainWithJsonWhenRequestHeaderIsNull(){
        grafanaService.executeSparkJob(JSON, null);
        verifyStatic();
        SparkSubmit.main(EXPECTED_ARGUMENTS_JSON);
    }

    @Test
    public void shouldCallSparkSubmitMainWithSql(){
        grafanaService.executeSparkJob(JOB_START, JOB_END, METRIC, AGGREGATOR, TAGS);
    }

    @Test
    public void shouldReturnBadQueryWhenAnyArgumentIsNull(){
        String result = grafanaService.executeSparkJob(JOB_START, JOB_END, null, AGGREGATOR, TAGS);
        assertEquals(result,"Bad query");
        result = grafanaService.executeSparkJob(JOB_START, null, METRIC, AGGREGATOR, TAGS);
        assertEquals(result, "Bad query");
        result = grafanaService.executeSparkJob(JOB_START, JOB_END, METRIC, null, TAGS);
        assertEquals(result, "Bad query");
        result = grafanaService.executeSparkJob(JOB_START, JOB_END, METRIC, AGGREGATOR, null);
        assertEquals(result, "Bad query");
    }

    @Test
    public void shouldReturnBadQueryWhenTimestampIsNotNumber(){
        String result = grafanaService.executeSparkJob(JOB_START, "AB", METRIC, AGGREGATOR, TAGS);
        assertEquals(result,"Bad query");
        result = grafanaService.executeSparkJob("44AD", JOB_END, METRIC, AGGREGATOR, TAGS);
        assertEquals(result,"Bad query");
    }

    @Test
    public void shouldReturnBadQueryWhenAggregatorIsNotValid(){
        String result = grafanaService.executeSparkJob(JOB_START, JOB_END, METRIC, "BAD", TAGS);
        assertEquals(result,"Bad query");
    }

    @Test
    public void shouldCloseSparkContext(){
        Response response = grafanaService.closeSparkContext();
        assertEquals(response.getStatus(), 200);
    }
}