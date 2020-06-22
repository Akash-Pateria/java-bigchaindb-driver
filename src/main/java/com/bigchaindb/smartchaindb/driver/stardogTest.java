package com.bigchaindb.smartchaindb.driver;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.*;
import com.complexible.stardog.api.ConnectionPool;
import com.stardog.stark.query.SelectQueryResult;

import java.util.*;
import java.util.HashMap;

public class stardogTest {

    public static void main(String[] args) throws Exception {
        // getKeys();
        getRandomValues("Description");
    }

    static List<String> getKeys() {
        // function to get random keys for the request and assign it to the bigchain
        // metadata
        List<String> randomKeys = new ArrayList<>();
        try {
            ConnectionPool connectPool = DBConnectionPool.getInstance();

            try (Connection connect = connectPool.obtain()) {
                try {
                    SelectQuery squery = connect.select(
                            "select ?o1 where \n" + "{  ?s rdfs:domain ?o1.\n" + "   ?s rdfs:range ?o2 .\n" + "}");

                    SelectQueryResult sresult = squery.execute();
                    // System.out.println("First 10 results for the query");
                    // QueryResultWriters.write(sresult, System.out,
                    // TextTableQueryResultWriter.FORMAT);
                    // List<String> keys = new ArrayList<>();
                    HashMap<String, String> keys = new HashMap<>();

                    while (sresult.hasNext()) {
                        String temp = sresult.next().get("o1").toString();
                        // System.out.print(temp.substring(42) + "------------------------------");
                        if (!temp.substring(42).equals("Material") && !temp.substring(42).equals("Quantity")) {
                            // System.out.println(temp);
                            keys.put(temp, temp.substring(42));
                        }
                    }
                    // while(sresult.hasNext()) {
                    // String temp = sresult.next().get("o2").toString();
                    // if(!temp.substring(42).equals("Material") &&
                    // !temp.substring(42).equals("Quantity") &&
                    // !temp.substring(42).equals("Description") &&
                    // !temp.substring(42).equals("MachiningFunction")) {
                    //// System.out.println(temp);
                    // keys.put(temp, temp.substring(42));
                    // }
                    // }

                    Random rand = new Random();
                    int numOfKeys = rand.nextInt(6) + 2; // varying number of attributes
                    Object[] keySet = keys.keySet().toArray();

                    for (int i = 0; i < numOfKeys; i++) {
                        int index = rand.nextInt(keySet.length);
                        randomKeys.add(keys.get(keySet[index])); // add those random key generated into the
                                                                 // randomKeys
                    }

                    for (int i = 0; i < randomKeys.size(); i++) {
                        System.out.println("KEYS FOR METADATA ---------- " + randomKeys.get(i));
                        // prints all the keys generated for the metadata
                    }

                } finally {
                    try {
                        connectPool.release(connect);
                    } catch (StardogException e) {
                        e.printStackTrace();
                    }
                    // connectPool.shutdown();
                }

            }
        } catch (StardogException e) {
            e.printStackTrace();
        }
        return randomKeys;
    }

    static String getMaterial() {
        // function to get random value for the key " Material " and returning it into
        // the bigchain metadata

        // Stardog aStardog = Stardog.builder().create();
        String material = null;
        try {
            ConnectionPool connectPool = DBConnectionPool.getInstance();

            try (Connection connect = connectPool.obtain()) {
                try {
                    SelectQuery squery = connect.select("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
                            + "SELECT ?y where{ ?y rdf:type <http://www.manunetwork.com/manuservice/v1#Material>\n"
                            + "}");

                    SelectQueryResult sresult = squery.execute();
                    // System.out.println("First 10 results for the query");
                    // QueryResultWriters.write(sresult, System.out,
                    // TextTableQueryResultWriter.FORMAT);
                    List<String> mat = new ArrayList<>();
                    while (sresult.hasNext()) {
                        mat.add(sresult.next().get("y").toString());
                    }

                    Random rand = new Random();
                    int num = rand.nextInt(mat.size());
                    // System.out.println(material + " " +num);
                    // for(String m:mat){
                    // System.out.println(m);
                    // }
                    material = mat.get(num).substring(42);
                } finally {
                    try {
                        connectPool.release(connect);
                    } catch (StardogException e) {
                        e.printStackTrace();
                    }
                    // connectPool.shutdown();
                }
            }
        } catch (StardogException e) {
            e.printStackTrace();
        }
        return material;
    }

    static String getQuantity() {
        // function to get random value for the key " Quantity " and returning it into
        // the bigchain metadata
        Random rand = new Random();
        int num = rand.nextInt(10000);
        return Integer.toString(num);
    }

    static String getRandomValues(String key) {

        // function to get random values for the key which is passed as an argument. The
        // key here is one of the attributes in the bigchaoin metadata
        String value = null;
        try {
            ConnectionPool connectPool = DBConnectionPool.getInstance();

            try (Connection connect = connectPool.obtain()) {
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("<http://www.manunetwork.com/manuservice/v1#").append(key).append(">");
                    // System.out.println(sb.toString());
                    StringBuilder query = new StringBuilder();
                    query.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ");
                    query.append("SELECT ?y where{ ?y rdfs:domain ").append(sb.toString());
                    query.append("}");
                    // System.out.println(query.toString());
                    SelectQuery squery = connect.select(query.toString());

                    SelectQueryResult sresult = squery.execute();
                    List<String> values = new ArrayList<>();
                    while (sresult.hasNext()) {
                        // System.out.print("VALUE -------------------");
                        // System.out.println(sresult.next().resource("y").get().toString());
                        values.add(sresult.next().resource("y").get().toString());
                    }
                    Random rand = new Random();
                    int index = rand.nextInt(values.size());
                    String retVal = values.get(index);

                    value = retVal.substring(42);
                } finally {
                    try {
                        connectPool.release(connect);
                    } catch (StardogException e) {
                        e.printStackTrace();
                    }
                    // connectPool.shutdown();
                }
            }
        } catch (StardogException e) {
            e.printStackTrace();
        }
        return value;
    }
}
