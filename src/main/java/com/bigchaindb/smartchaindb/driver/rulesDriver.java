package com.bigchaindb.smartchaindb.driver;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.*;
import com.complexible.stardog.api.ConnectionPool;
import com.stardog.stark.query.BindingSet;
import com.stardog.stark.query.SelectQueryResult;

import java.util.*;

public class rulesDriver {

    public static void main(String[] args) {
        // List<String> temp = new ArrayList<>();
        // temp.add("Material");
        // Map<String, String> a = null;
        // List<String> ret = getCapabilities(temp,a);
        System.out.println("Rules");
    }

    static Map<String, String> genratedRules() {
        // generates random rules (rule genreation)
        return new TreeMap<String, String>();
    }

    static List<String> getCapabilities(List<String> keys, Map<String, String> metaMap) {
        // check the meta attributes with the rules generated before and assigning them
        // to a topic.
        List<String> topics = new ArrayList<>(Capabilities.getAll());
        List<String> capability = new ArrayList<>();
        Random rand = new Random();

        // random number of rules
        int numOfRules = rand.nextInt(50) + 30;
        System.out.println("\n\nNumber of Rules: " + numOfRules + "\n\n");
        for (int i = 0; i < numOfRules; i++) {
            // random number of keys
            HashMap<String, String> checkVal = new HashMap<>();

            int numOfKeys = rand.nextInt(keys.size());
            System.out.println("Rule: " + i + " Number of keys : " + numOfKeys);
            for (int j = 0; j < numOfKeys; j++) {
                // random key
                int index = rand.nextInt(keys.size());
                String key = keys.get(index);
                // random value for each key
                System.out.println(" Rules key ----------  " + key);
                String value;
                if (key.equals("Quantity")) {
                    value = Integer.toString(rand.nextInt(100000));
                } else if (key.equals("Material")) {
                    value = stardogTest.getMaterial();
                } else {
                    value = stardogTest.getRandomValues(key);
                }
                System.out.println("Rules Value --------- " + value);
                checkVal.put(key, value);
            }
            // check with metadata
            boolean check = true;
            for (Map.Entry<String, String> m : checkVal.entrySet()) {
                if (m.getKey() != "Quantity") {
                    if (metaMap.get(m.getKey()) != m.getValue()) {
                        check = false;
                        break;
                    }
                } else {
                    if (Integer.parseInt(metaMap.get("Quantity")) < 1000) {
                        check = false;
                        break;
                    }
                }
            }
            // random number of topics
            HashSet<String> capSet = new HashSet<>();
            if (check) {
                int numOfCap = rand.nextInt(topics.size()) + 1;
                for (int k = 0; k < numOfCap; k++) {
                    int index = rand.nextInt(topics.size());
                    capSet.add(topics.get(index));
                }
                capability.addAll(capSet);
                break;
            }
        }
        if (capability.isEmpty()) {
            capability.add(Capabilities.MISC);
        }
        System.out.println("\n\nExiting rules engine\n\n");
        return capability;
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

        String matchedTopic = inferredCapability;
        final String prefix = "<http://resourcedescription.tut.fi/ontology/processTaxonomyModel#";

        try {
            ConnectionPool connectPool = DBConnectionPool.getInstance();
            try (Connection connect = connectPool.obtain()) {
                try {
                    StringBuilder query = new StringBuilder();
                    // query.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
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

                    System.out.println("\n\nMATCHED CAPABILITY TOPIC: " + matchedTopic + "\n");
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
}
