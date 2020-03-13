package com.bigchaindb.smartchaindb.driver;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONArray;
import org.json.JSONObject;


public class ConsumerDriver {
    public static void main(String[] args) {

        runConsumer();
    }
//    static void runConsumer() {
//        Consumer<String, String> consumer = ConsumerCreator.createRequestConsumer();
//
//        consumer.subscribe(Collections.singletonList(Capabilities.PLASTIC));
//
//        List<JSONObject> RequestList = new ArrayList<JSONObject>();
//        int noMessageFound = 0;
//        while (true) {
//          ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
//          // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
//          if (consumerRecords.count() == 0) {
//              noMessageFound++;
//              if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
//                // If no message found count is reached to threshold exit loop.
//                break;
//              }
//              else
//                  continue;
//          }
//          //print each record.
//          consumerRecords.forEach(record -> {
//              System.out.println("Record Key " + record.key());
//              System.out.println("Record value " + record.value());
//              System.out.println("Record partition " + record.partition());
//              System.out.println("Record offset " + record.offset());
//
//              JSONObject jsonReq = new JSONObject(record.value());
//
//              RequestList.add(jsonReq);
//           });
//          // commits the offset of record to broker.
//           consumer.commitAsync();
//
//        }
//    consumer.close();
//
//    System.out.println("List of requests: "+RequestList);
//    }

    static void runConsumer() {
        // function to create a consumer group which polls at a interval of time and check the record with its capabilities and if matched it simply adds in to RequestList.

        Consumer<String, String> consumer = ConsumerCreator.createRequestConsumer();
        List<String> topicsToSubscribe = Arrays.asList(Capabilities.PLASTIC,Capabilities.MILLING);
        consumer.subscribe(topicsToSubscribe);

        HashSet<String> checkTopics = new HashSet<>();
        for(String l:topicsToSubscribe){
            checkTopics.add(l);
        }
        HashSet<String> checkRequest = new HashSet<>();        // hash set to store the transaction id it has already check.
        AtomicBoolean addRequest = new AtomicBoolean(true);
        List<JSONObject> RequestList = new ArrayList<>();       //  List ot store all the request it has completely matched with its capabilities.
        int noMessageFound=0;
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if(consumerRecords.count()== 0) {
                noMessageFound++;
                if(noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
                    // If no message found count is reached to threshold exit loop.
                    break;
                }
                else
                    continue;
            }

            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
                JSONObject jsonReq = new JSONObject(record.value());

//                check the transaction id is already present in checkRequest, if so drop or check the capabilities
                if(!checkRequest.contains(jsonReq.get("Transaction_id"))) {
                    //check all topics are present in checktopics
                    JSONArray reqCapabilities = jsonReq.getJSONArray("Capability");

                    //System.out.println("Id:" + jsonReq.get("Transaction_id") );
                    for(int i=0;i<reqCapabilities.length();i++) {
                        if (!checkTopics.contains(reqCapabilities.get(i))) {
                            addRequest.set(false);
                            break;
                       }
                    }
                    boolean value = addRequest.get();
                    if(value == true) {
                        // add in requestList and add the request in checkRequest
                        checkRequest.add((String) jsonReq.get("Transaction_id"));
                        RequestList.add(jsonReq);
                    }
                }
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();

        System.out.println("List of requests: "+RequestList);
    }
}