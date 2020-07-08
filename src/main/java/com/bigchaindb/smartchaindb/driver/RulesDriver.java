package com.bigchaindb.smartchaindb.driver;

import java.util.*;

public class RulesDriver {

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
        int numOfRules = rand.nextInt(30) + 20;
        System.out.println("Total no. of rules: " + numOfRules);
        for (int i = 0; i < numOfRules; i++) {
            // random number of keys
            System.out.println("Trying rule#" + (i + 1));
            HashMap<String, String> checkVal = new HashMap<>();

            int numOfKeys = rand.nextInt(keys.size());
            for (int j = 0; j < numOfKeys; j++) {
                // random key
                int index = rand.nextInt(keys.size());
                String key = keys.get(index);
                // random value for each key
                String value;
                if (key.equals("Quantity")) {
                    value = Integer.toString(rand.nextInt(100000));
                } else if (key.equals("Material")) {
                    value = StardogTest.getMaterial();
                } else {
                    value = StardogTest.getRandomValues(key);
                }
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
                int numOfCap = rand.nextInt(3) + 1;
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

        System.out.println("Inferred Capabilities: " + capability.toString());
        return capability;
    }
}
