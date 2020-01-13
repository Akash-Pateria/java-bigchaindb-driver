package com.bigchaindb.smartchaindb.driver;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.*;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.query.SelectQueryResult;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class CoordinatorDriver {

    public static void main(String[] args) {
//          getIdForTopics(topicToIdMap);
    }

    static HashMap<String, Integer> getIdForTopics(HashMap<String, Integer> topicToIdMap){
        // function to store the topics and its corresponding ID's assigned to them. In this case, we have simply used and iterator to assign the id's to the topic.
        try {
            try (AdminConnection connection = AdminConnectionConfiguration.toServer("http://localhost:5820").credentials("admin", "admin").connect()) {


//                connection.list().forEach(item -> System.out.println(item));
                if (connection.list().contains("testDB")) {
//                    System.out.println("Database already present, So we are droping it");
                    connection.drop("testDB");
                }
                connection.disk("testDB").create();
                connection.close();

                ConnectionConfiguration connectConfig = ConnectionConfiguration.to("testDB").server("http://localhost:5820").credentials("admin", "admin");

                ConnectionPoolConfig connectPoolConfig = ConnectionPoolConfig.using(connectConfig).minPool(10).maxPool(200).expiration(300, TimeUnit.SECONDS).blockAtCapacity(900, TimeUnit.SECONDS);

                ConnectionPool connectPool = connectPoolConfig.create();

                try (Connection connect = connectPool.obtain()) {
                    try {
                        connect.begin();
                        connect.add().io().format(RDFFormats.RDFXML).stream(new FileInputStream("src/main/resources/ManuServiceOntology.xml"));   // ontology
                        connect.commit();


                        SelectQuery squery = connect.select("select DISTINCT ?o {\n" +
                                "    ?s rdfs:domain ?o\n" +
                                "}");

                        SelectQueryResult sresult = squery.execute();

                        int i=0;
                        while (sresult.hasNext()){
                            String temp = sresult.next().get("o").toString();
                            topicToIdMap.put(temp.substring(42),i);
                            i++;
                        }

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            connectPool.release(connect);
                        } catch (StardogException e) {
                            e.printStackTrace();
                        }
                        connectPool.shutdown();
                    }
                }
            }
        }finally {
//            aStardog.shutdown();
        }
        return topicToIdMap;
    }

    static void addManufacture(){
              // function to create a consumer group manufacturers in Kafka
              ConsumerDriver.runConsumer();
    }
}
