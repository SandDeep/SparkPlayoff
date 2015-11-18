package com.poc.mongo;

import java.io.Serializable;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoUtil implements Serializable{

	private static final long serialVersionUID = -5940798222639717237L;

	public static DBCollection getCollection(String dbName,
			String CollectionName) {
		
		MongoClient mongo = MongoClientSingleton.getInstance();
		DB db = mongo.getDB(dbName);
		DBCollection collection = db.getCollection(CollectionName);

		return collection;
	}
}
