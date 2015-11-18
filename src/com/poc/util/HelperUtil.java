package com.poc.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class HelperUtil implements Serializable{
	
	private static final long serialVersionUID = -112838773971412692L;
	
	private static final Logger logger = Logger.getLogger(HelperUtil.class);
	
	/**
	 * Function to Convert saved Kafka JSON String to Map.
	 * 
	 * @param msg
	 * @return
	 */
	public static Map<String, Object> convertToMap(String msg) {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> map;

		try {
			map = mapper.readValue(msg,new TypeReference<HashMap<String, Object>>() {});
			logger.debug(map);
			
		} catch (IOException e) {
			logger.error(e.getMessage());
			return null;
		}
		return map;
	}
}
