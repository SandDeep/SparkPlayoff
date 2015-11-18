package com.poc.cluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerTest implements Runnable {

	private KafkaStream<byte[], byte[]> stream;
	private int threadNumber;

	public ConsumerTest(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {
		this.stream = a_stream;
		this.threadNumber = a_threadNumber;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

		while (iterator.hasNext()) {
			String msg=new String(iterator.next().message());
			System.out.println("Thread " + threadNumber + ": "
					+ msg);
			convertToMap(msg);
		}

		System.out.println("Shutting Down Thread : " + threadNumber);
	}

	private void convertToMap(String msg) {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> map = new HashMap<>();

		try {
			map = mapper.readValue(msg,
					new TypeReference<HashMap<String, Object>>() {
					});
			System.out.println(map);
			int pgTime=(int) map.get("pgAccess5Min");
			String url=(String) map.get("url");
			System.out.println(url);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
