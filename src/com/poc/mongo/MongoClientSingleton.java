package com.poc.mongo;

import java.io.Serializable;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.poc.util.Config;

/***
 * SingleTon class to provide instance of MongoDB Client.
 *
 * @author vibhav
 *
 */
public class MongoClientSingleton implements Serializable {
	private static final Logger logger = Logger.getLogger(MongoClientSingleton.class);
	
	private static final long serialVersionUID = -7604766932017737115L;
    private static MongoClient mongoClient;

    /***
     * Private constructor : to prevent creation of object of this class
     */
    private MongoClientSingleton() {
        if (mongoClient != null) {
            throw new InstantiationError("Instance creation not allowed");
        }
    }


    /***
     * Method provides instance of mongoDB Client Procedure -
     * 	1. Checks if static instance is null or not (if not null, return the existing).
     * 	2. Double checks for the existence of the same instance in synchronized
     * 		block.
     *  3. If instance is found null, creates new with host and port from
     * 		DBUtil class.
     *
     * @return MongoClient instance
     */
    public static MongoClient getInstance() {
        if (mongoClient == null) {
            synchronized (MongoClientSingleton.class) {
                if (mongoClient == null) {
                    try {
                        mongoClient = new MongoClient(Config.LOCAL_DB_SERVER_ADDRESS,
                                MongoClientOptions.builder().connectionsPerHost(10)
                                        .threadsAllowedToBlockForConnectionMultiplier(15)
                                        .connectTimeout(5000).writeConcern(WriteConcern.NORMAL)
                                        .build());
                    } catch (UnknownHostException e) {
                    	logger.error("Mongo Setup Error" + e.getMessage());
                    }
                }
            }
        }
        return mongoClient;
    }

    /***
     * Method to handle object creation problem on de-serialisation
     *
     * @return
     */
    protected Object readResolve() {
        return getInstance();
    }

    /***
     * Method to handle object creation through Cloning
     */
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("Clone not allowed");
    }
}