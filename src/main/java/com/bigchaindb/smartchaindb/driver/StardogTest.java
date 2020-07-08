package com.bigchaindb.smartchaindb.driver;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.*;
import com.complexible.stardog.api.ConnectionPool;
import com.stardog.stark.query.SelectQueryResult;
import com.stardog.stark.query.BindingSet;

import java.util.*;

public class StardogTest {

    private static List<String> capabilityList;
    private static List<String> keyList;
    private static List<String> valueList;
    private static List<String> materialList;

    private static final String prefix = "<http://resourcedescription.tut.fi/ontology/processTaxonomyModel#";

    static List<String> getKeys() {
        // function to get random keys for the request and assign it to the bigchain
        // metadata
        if (keyList == null || keyList.isEmpty()) {
            try {
                ConnectionPool connectPool = DBConnectionPool.getInstance();
                try (Connection connect = connectPool.obtain()) {
                    try {
                        SelectQuery squery = connect.select(
                                "select ?o1 where \n" + "{  ?s rdfs:domain ?o1.\n" + "   ?s rdfs:range ?o2 .\n" + "}");

                        SelectQueryResult sresult = squery.execute();

                        Set<String> keySet = new HashSet<>();
                        while (sresult.hasNext()) {
                            String temp = sresult.next().get("o1").toString();
                            if (!temp.substring(42).equals("Material") && !temp.substring(42).equals("Quantity")) {
                                keySet.add(temp.substring(42));
                            }
                        }

                        keyList = new ArrayList<>(keySet);
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
        }

        List<String> randomKeys = new ArrayList<>();
        Random rand = new Random();
        // varying number of attributes
        int numOfKeys = rand.nextInt(6) + 2;

        for (int i = 0; i < numOfKeys; i++) {
            int index = rand.nextInt(keyList.size());
            randomKeys.add(keyList.get(index));
        }

        return randomKeys;
    }

    static String getMaterial() {
        // function to get random value for the key " Material " and returning it into
        // the bigchain metadata
        if (materialList == null || materialList.isEmpty()) {
            try {
                ConnectionPool connectPool = DBConnectionPool.getInstance();
                try (Connection connect = connectPool.obtain()) {
                    try {
                        SelectQuery squery = connect.select("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
                                + "SELECT ?y where{ ?y rdf:type <http://www.manunetwork.com/manuservice/v1#Material>\n"
                                + "}");

                        SelectQueryResult sresult = squery.execute();

                        materialList = new ArrayList<>();
                        while (sresult.hasNext()) {
                            materialList.add(sresult.next().get("y").toString().substring(42));
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
        }

        int randIndex = new Random().nextInt(materialList.size());
        return materialList.get(randIndex);
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
        if (valueList == null || valueList.isEmpty()) {
            try {
                ConnectionPool connectPool = DBConnectionPool.getInstance();
                try (Connection connect = connectPool.obtain()) {
                    try {
                        StringBuilder sb = new StringBuilder();
                        sb.append("<http://www.manunetwork.com/manuservice/v1#").append(key).append(">");
                        StringBuilder query = new StringBuilder();
                        query.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ");
                        query.append("SELECT ?y where{ ?y rdfs:domain ").append(sb.toString());
                        query.append("}");

                        SelectQuery squery = connect.select(query.toString());
                        SelectQueryResult sresult = squery.execute();

                        valueList = new ArrayList<>();
                        while (sresult.hasNext()) {
                            valueList.add(sresult.next().resource("y").get().toString().substring(42));
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
        }

        Random rand = new Random();
        int index = rand.nextInt(valueList.size());

        return valueList.get(index);
    }

    public static void reasoning() {

        final String prefix = "<http://www.manunetwork.com/manuservice/v1#";

        final List<List<String>> requestLists = Arrays.asList(
                Arrays.asList("maxDepthOfCut", "diameterOfPlanarFace", "stockInAxialDirection",
                        "diameterOfInnerCylinder"),
                Arrays.asList("maxDepthOfCut", "diameterOfHole", "maxDiameterOfHole", "minDiameterOfHole",
                        "lengthOfHole", "diameterOfPredrilledHole"),
                Arrays.asList("radiusOfSmallestConcaveProfileBlend", "angleOfRisingFlank", "lengthOfProfile",
                        "maxDepthOfCut", "stockInRadialDirection", "qualityOfFace"),
                Arrays.asList("depthOfSlot", "widthOfSlot", "maxWidthOfSlot", "minWidthOfSlot", "angleOfWallValue",
                        "qualityOfWallSurface"),
                Arrays.asList("diameterOfPlanarSplitFace", "diameterOfInnerHole", "maxWidthOfRemovedMaterialZone"));
        try {
            ConnectionPool connectPool = DBConnectionPool.getInstance();
            try (Connection connect = connectPool.obtain()) {
                try {
                    for (List<String> requestList : requestLists) {

                        StringBuilder query = new StringBuilder();
                        query.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
                        query.append("SELECT ?feature ?superfeature \nWHERE { \n");

                        System.out.println("REQUEST PARAMS:");
                        for (String param : requestList) {
                            System.out.print(param + "; ");
                            query.append(prefix + param + ">").append(" rdfs:domain ?feature .\n");
                        }

                        query.append("?feature rdfs:subClassOf ?superfeature .\n");
                        query.append("}");

                        SelectQuery squery = connect.select(query.toString());
                        SelectQueryResult sresult = squery.execute();

                        String inferred_process = "ERROR: Could not infer!";

                        System.out.println("\n\nINFERRED FEATURES:");
                        while (sresult.hasNext()) {
                            BindingSet current_row = sresult.next();

                            inferred_process = current_row.resource("superfeature").get().toString();
                            System.out.println(current_row.resource("feature").get().toString().substring(42));
                        }

                        System.out.println("\nINFERRED MANUFACTURING PROCESS: \n" + inferred_process.substring(42)
                                + "\n------------------------------------------------------------");
                    }
                } finally {
                    try {
                        connectPool.release(connect);
                    } catch (StardogException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (StardogException e) {
            e.printStackTrace();
        }
    }

    public static String getCapabilityTopic(String inferredCapability) {
        HashSet<String> topics = Capabilities.getAll();
        if (topics.contains(inferredCapability))
            return inferredCapability;

        String matchedTopic = Capabilities.MISC;
        try {
            ConnectionPool connectPool = DBConnectionPool.getInstance();
            try (Connection connect = connectPool.obtain()) {
                try {
                    StringBuilder query = new StringBuilder();

                    query.append("SELECT ?superclass (count(?mid) as ?rank) \nWHERE { \n");
                    query.append(prefix + inferredCapability + ">").append(" rdfs:subClassOf* ?mid .\n");
                    query.append("?mid rdfs:subClassOf* ?superclass .\n");
                    query.append("}\n");
                    query.append("group by ?superclass\n");
                    query.append("order by ?rank");

                    SelectQuery squery = connect.select(query.toString());
                    SelectQueryResult sresult = squery.execute();

                    while (sresult.hasNext()) {
                        String current_superclass = sresult.next().resource("superclass").get().toString()
                                .substring(64);
                        if (current_superclass != inferredCapability && topics.contains(current_superclass)) {
                            matchedTopic = current_superclass;
                            break;
                        }
                    }
                } finally {
                    try {
                        connectPool.release(connect);
                    } catch (StardogException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (StardogException e) {
            e.printStackTrace();
        }

        return matchedTopic;
    }

    public static List<String> getRandomCapability() {
        populateCapabilityList();

        Random rand = new Random();
        List<String> randomCapabilityList = new ArrayList<>();
        int randCount = rand.nextInt(3) + 1;

        for (int i = 0; i < randCount; i++) {
            int randIndex = rand.nextInt(capabilityList.size());
            randomCapabilityList.add(capabilityList.get(randIndex));
        }

        return randomCapabilityList;
    }

    private static void populateCapabilityList() {
        if (capabilityList == null) {
            capabilityList = new ArrayList<>();
            try {
                ConnectionPool connectPool = DBConnectionPool.getInstance();
                try (Connection connect = connectPool.obtain()) {
                    try {
                        StringBuilder query = new StringBuilder();

                        query.append("SELECT ?subclass WHERE {\n");
                        query.append("?subclass rdfs:subClassOf* ?intermediate .\n");
                        query.append("?intermediate rdfs:subClassOf* ").append(prefix + "ProcessTaxonomyElement> .\n");
                        query.append("}\n");
                        query.append("group by ?subclass\n");
                        query.append("HAVING (count(?intermediate)-1 > 1)");

                        SelectQuery squery = connect.select(query.toString());
                        SelectQueryResult sresult = squery.execute();

                        while (sresult.hasNext()) {
                            capabilityList.add(sresult.next().resource("subclass").get().toString().substring(64));
                        }
                    } finally {
                        try {
                            connectPool.release(connect);
                        } catch (StardogException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (StardogException e) {
                e.printStackTrace();
            }
        }
    }
}
