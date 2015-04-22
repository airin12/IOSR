package agh.edu.pl.iosr_data_generator;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import pl.edu.agh.iosr.data.DataSample;
import pl.edu.agh.iosr.http.HTTPRequestSender;
import pl.edu.agh.iosr.worker.Worker;

import com.google.gson.Gson;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Random random = new Random(new Date().getTime());
        Map<String,String> map = new HashMap<String,String>();
        //map.put("osoba", "ty");
        //map.put("exp", "exp1");
        map.put("cpu", "0");
        DataSample sample = new DataSample("mem.usage.perc", new Date().getTime(),random.nextDouble(), map);
        Gson gson = new Gson();
        System.out.println(" template: "+gson.toJson(sample));
        
        HTTPRequestSender sender = new HTTPRequestSender("http://localhost:2002/api/put");
        
        Runnable runnable = new Worker(10, 5000, 0.0, 100.0, sample, sender,3);
        Thread thread = new Thread(runnable);
        
        thread.start();
        try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
    }
}
