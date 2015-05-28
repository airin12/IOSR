package pl.edu.agh.iosr.http;

import java.io.IOException;
import java.io.OutputStreamWriter;
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

}
