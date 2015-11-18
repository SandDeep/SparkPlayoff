package com.poc.cluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class Test {

	public static void main(String[] args) {
		//String msg="{\"url\": \"http:\/\/www.beautypageants.indiatimes.com\/news\/india\/8-pictures-of-jallianwala-bagh-that-will-leave-you-teary-eyed-346235.html\"}";
		convertToMap("");
	}
	private static void convertToMap(String msg) {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, String> map = new HashMap<>();

		try {
			map = mapper.readValue(msg,
					new TypeReference<HashMap<String, String>>() {
					});
			System.out.println(map);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
