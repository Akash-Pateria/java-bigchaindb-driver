package com.bigchaindb.smartchaindb.driver;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.*;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.query.SelectQueryResult;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

public class stardogTest {

    public static void main(String[] args) throws Exception {
//        getKeys();
        getRandomValues("Description");
    }
    static List<String> getKeys(){
        // function to get random keys for the request and assign it to the bigchain metadata
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

                com.complexible.stardog.api.ConnectionPool connectPool = connectPoolConfig.create();

                try (Connection connect = connectPool.obtain()) {
                    try {
                        connect.begin();
                        connect.add().io().format(RDFFormats.RDFXML).stream(new FileInputStream("src/main/resources/ManuServiceOntology.xml"));
                        connect.commit();


                        SelectQuery squery = connect.select("select ?o1 where \n" +
                                "{  ?s rdfs:domain ?o1.\n" +
                                "   ?s rdfs:range ?o2 .\n" +
                                "}");

                        SelectQueryResult sresult = squery.execute();
//                        System.out.println("First 10 results for the query");
//                        QueryResultWriters.write(sresult, System.out, TextTableQueryResultWriter.FORMAT);
//                        List<String> keys = new ArrayList<>();
                        HashMap<String,String> keys = new HashMap<>();

                        while(sresult.hasNext()) {
                            String temp = sresult.next().get("o1").toString();
//                            System.out.print(temp.substring(42) + "------------------------------");
                            if(!temp.substring(42).equals("Material") && !temp.substring(42).equals("Quantity")) {
//                                System.out.println(temp);
                                keys.put(temp, temp.substring(42));
                            }
                        }
//                        while(sresult.hasNext()) {
//                            String temp = sresult.next().get("o2").toString();
//                            if(!temp.substring(42).equals("Material") && !temp.substring(42).equals("Quantity") && !temp.substring(42).equals("Description") && !temp.substring(42).equals("MachiningFunction")) {
////                                System.out.println(temp);
//                                keys.put(temp, temp.substring(42));
//                            }
//                        }

                        Random rand = new Random();
                        int numOfKeys = rand.nextInt(6) + 2;        // varying number of attributes
                        Object[] keySet = keys.keySet().toArray();
                        List<String> randomKeys = new ArrayList<>();
                        for(int i=0;i<numOfKeys;i++){
                            int index = rand.nextInt(keySet.length);
                            randomKeys.add(keys.get(keySet[index]));        // add those random key generated into the randomKeys
                        }

                        for(int i=0;i<randomKeys.size();i++){
                            System.out.println("KEYS FOR METADATA ---------- " + randomKeys.get(i));            // prints all the keys generated for the metadata
                        }
                        return randomKeys;

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

        return null;
    }
    static String getMaterial(){
         // function to get random value for the key " Material " and returning it into the bigchain metadata

//        Stardog aStardog = Stardog.builder().create();

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

                com.complexible.stardog.api.ConnectionPool connectPool = connectPoolConfig.create();

                try (Connection connect = connectPool.obtain()) {
                    try {
                        connect.begin();
                        connect.add().io().format(RDFFormats.RDFXML).stream(new FileInputStream("src/main/resources/ManuServiceOntology.xml"));
                        connect.commit();


                        SelectQuery squery = connect.select("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
                                "SELECT ?y where{ ?y rdf:type <http://www.manunetwork.com/manuservice/v1#Material>\n" +
                                "}");

                        SelectQueryResult sresult = squery.execute();
//                        System.out.println("First 10 results for the query");
//                        QueryResultWriters.write(sresult, System.out, TextTableQueryResultWriter.FORMAT);
                        List<String> mat = new ArrayList<>();
                        while(sresult.hasNext()) {
                            mat.add(sresult.next().get("y").toString());
                        }

                        Random rand = new Random();
                        int num = rand.nextInt(mat.size());
                        String material = mat.get(num);
//                        System.out.println(material + " " +num);
    //                    for(String m:mat){
    //                        System.out.println(m);
    //                    }
                        return material.substring(42);
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
        return null;
    }
    static String getQuantity(){
        // function to get random value for the key " Quantity " and returning it into the bigchain metadata

        Random rand = new Random();
         int num = rand.nextInt(10000);
         return Integer.toString(num);
    }

    static String getRandomValues(String key){

        // function to get random values for the key which is passed as an argument. The key here is one of the attributes in the bigchaoin metadata
        try {
            try (AdminConnection connection = AdminConnectionConfiguration.toServer("http://localhost:5820").credentials("admin", "admin").connect()) {


                connection.list().forEach(item -> System.out.println(item));
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
                        connect.add().io().format(RDFFormats.RDFXML).stream(new FileInputStream("src/main/resources/ManuServiceOntology.xml"));
                        connect.commit();

                        StringBuilder sb = new StringBuilder();
                        sb.append("<http://www.manunetwork.com/manuservice/v1#").append(key).append(">");
//                        System.out.println(sb.toString());
                        StringBuilder query = new StringBuilder();
                        query.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ");
                        query.append("SELECT ?y where{ ?y rdfs:domain ").append(sb.toString());
                        query.append("}");
//                        System.out.println(query.toString());
                        SelectQuery squery = connect.select(query.toString());

                        SelectQueryResult sresult = squery.execute();
                        List<String> value = new ArrayList<>();
                        while(sresult.hasNext()) {
//                            System.out.print("VALUE -------------------");
//                            System.out.println(sresult.next().resource("y").get().toString());
                          value.add(sresult.next().resource("y").get().toString());
                        }
                        Random rand = new Random();
                        int index = rand.nextInt(value.size());
                        String retVal = value.get(index);

                        return retVal.substring(42);
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
        return null;
    }
}

