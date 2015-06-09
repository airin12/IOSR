package pl.edu.agh.iosr.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class HTTPRequestSender {

	private String address;

	public HTTPRequestSender(String address) {
		super();
		this.address = address;
	}

	public HTTPRequestSender() {

	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int sendDataSample(String json) {

		URL url;
		try {
			url = new URL(address);

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			connection.setDoOutput(true);

			OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
			wr.write(json);
			wr.flush();
			wr.close();

			int responseCode = connection.getResponseCode();
			return responseCode;

		} catch (MalformedURLException e) {
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
	}

	public String sendTestCaseOneRequest() {
		URL url;
		try {
			url = new URL(address);
			System.out.println(url);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			connection.getResponseCode();

			StringWriter writer = new StringWriter();

			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null)
				writer.write(inputLine);
			in.close();

			return writer.toString();

		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String sendTestCaseTwoRequest(String json) {
		URL url;
		try {
			url = new URL(address);

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			connection.setDoOutput(true);

			OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
			wr.write(json);
			wr.flush();
			wr.close();

			connection.getResponseCode();
			StringWriter writer = new StringWriter();

			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null)
				writer.write(inputLine);
			in.close();
			return writer.toString();

		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}
