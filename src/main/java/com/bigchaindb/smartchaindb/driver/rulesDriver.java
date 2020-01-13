package com.bigchaindb.smartchaindb.driver;

import java.util.*;

public class rulesDriver {

    public static void main(String[] args) {
//        List<String> temp = new ArrayList<>();
//        temp.add("Material");
//        Map<String, String> a = null;
//        List<String> ret = getCapabilities(temp,a);
          System.out.println("Rules");
    }
    static Map<String, String> genratedRules(){
        // generates random rules (rule genreation)
    }

    static List<String> getCapabilities(List<String> keys, Map<String, String> metaMap){
        // check the meta attributes with the rules generated before and assigning them to a topic.
        List<String> topics = Capabilities.getAll();
        List<String> capability = new ArrayList<>();
        Random rand = new Random();

        // random number of rules
        int numOfRules = rand.nextInt(50) + 30;
        for(int i=0;i<numOfRules;i++) {
            //random number of keys
            HashMap<String,String> checkVal = new HashMap<>();
            int numOfKeys = rand.nextInt(keys.size());
            for(int j=0;j<numOfKeys;j++) {
                //random key
                int index = rand.nextInt(keys.size());
                String key = keys.get(index);
                //random value for each key
                System.out.println(" Rules key ----------  " + key);
                String value;
                if(key.equals("Quantity")){
                    value = Integer.toString(rand.nextInt(100000));
                }
                else if(key.equals("Material")){
                    value = stardogTest.getMaterial();
                }else {
                    value = stardogTest.getRandomValues(key);
                }
                System.out.println("Rules Value --------- " + value);
                checkVal.put(key,value);
            }
            //check with metadata
            boolean check = true;
            for(Map.Entry m : checkVal.entrySet()) {
                if(m.getKey()!="Quantity") {
                    if (metaMap.get(m.getKey()) != m.getValue()) {
                        check = false;
                        break;
                    }
                }else{
                    if(Integer.parseInt(metaMap.get("Quantity")) < 1000){
                         check=false;
                         break;
                    }
                }
            }
            //random number of topics
            HashSet<String> capSet = new HashSet<> ();
            if(check){
                int numOfCap = rand.nextInt(topics.size())+1;
                for(int k=0;k<numOfCap;k++){
                     int index = rand.nextInt(topics.size());
                     capSet.add(topics.get(index));
                }
                capability.addAll(capSet);
                break;
            }
        }
        if(capability.isEmpty()){
            capability.add(Capabilities.MISC);
        }
        return capability;
    }
}
