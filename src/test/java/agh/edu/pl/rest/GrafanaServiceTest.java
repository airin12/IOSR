package agh.edu.pl.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import org.apache.spark.deploy.SparkSubmit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SparkSubmit.class)
public class GrafanaServiceTest {

    private static final String JOB_START = "START";
    private static final String JOB_END = "END";
    private static final String METRIC = "CPU";
    private static final String AGGREGATOR = "SUM";
    private static final String TAGS = "TAGS";
    private static final String JSON = "JSON";
    private static final String[] EXPECTED_ARGUMENTS = new String[]{"--class", "agh.edu.pl.spark.SparkJobRunner", "--deploy-mode", "client",
    "--master", "spark://172.17.84.76:7077", "/root/files/spark.jar", "BASIC", "START:END:CPU:SUM:TAGS"};
    private static final String[] EXPECTED_ARGUMENTS_JSON = new String[]{"--class", "agh.edu.pl.spark.SparkJobRunner", "--deploy-mode", "client",
            "--master", "spark://172.17.84.76:7077", "/root/files/spark.jar", "SQL", "JSON"};
    private GrafanaService grafanaService;


    @Before
    public void setUp(){
        grafanaService = new GrafanaService();
        GrafanaService.resultMap.put("job", new Object());
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
        grafanaService.executeSparkJob(JSON, null);
        verifyStatic();
        SparkSubmit.main(EXPECTED_ARGUMENTS_JSON);
    }

    @Test
    public void shouldReturnBadQueryWhenAnyArgumentIsNull(){
        String result = grafanaService.executeSparkJob(JOB_START, JOB_END, null, AGGREGATOR, TAGS);
        assertEquals(result,"Bad query");
        result = grafanaService.executeSparkJob(JOB_START, null, METRIC, AGGREGATOR, TAGS);
        assertEquals(result,"Bad query");
        result = grafanaService.executeSparkJob(JOB_START, JOB_END, METRIC, null, TAGS);
        assertEquals(result,"Bad query");
    }
}