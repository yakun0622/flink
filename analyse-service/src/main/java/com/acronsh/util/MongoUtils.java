package com.acronsh.util;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * Created by li on 2019/1/5.
 */
public class MongoUtils {

    private static String host = "10.211.55.12";
    private static MongoClient mongoClient = new MongoClient(host, 27017);


    public static Document findOne(String tableName, String database, String yearBaseType) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection mongoCollection = mongoDatabase.getCollection(tableName);
        Document doc = new Document();
        doc.put("info", yearBaseType);
        FindIterable<Document> iterable = mongoCollection.find(doc);
        MongoCursor<Document> mongoCursor = iterable.iterator();
        if (mongoCursor.hasNext()) {
            return mongoCursor.next();
        } else {
            return null;
        }
    }


    public static void saveOrUpdate(String tableName, String database, Document doc) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
        if (!doc.containsKey("_id")) {
            ObjectId objectid = new ObjectId();
            doc.put("_id", objectid);
            mongoCollection.insertOne(doc);
            return;
        }
        Document matchDocument = new Document();
        String objectid = doc.get("_id").toString();
        matchDocument.put("_id", new ObjectId(objectid));
        FindIterable<Document> findIterable = mongoCollection.find(matchDocument);
        if (findIterable.iterator().hasNext()) {
            mongoCollection.updateOne(matchDocument, new Document("$set", doc));
            try {
                System.out.println("come into saveOrUpdate ---- update---" + JSONObject.toJSONString(doc));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            mongoCollection.insertOne(doc);
            try {
                System.out.println("come into saveOrUpdate ---- insert---" + JSONObject.toJSONString(doc));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
