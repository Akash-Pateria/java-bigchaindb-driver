package com.bigchaindb.smartchaindb.driver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;


public class ConsumerDriver {
    public static void main(String[] args) {

    	runConsumer();
    }
    static void runConsumer() { 	
        Consumer<String, String> consumer = ConsumerCreator.createRequestConsumer();
        
        consumer.subscribe(Collections.singletonList(Capabilities.PLASTIC));
        
        List<JSONObject> RequestList = new ArrayList<JSONObject>();
        int noMessageFound = 0;
        while (true) {
          ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
          // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
          if (consumerRecords.count() == 0) {
              noMessageFound++;
              if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
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
              
              RequestList.add(jsonReq);
           });
          // commits the offset of record to broker. 
           consumer.commitAsync();  
           
        }
    consumer.close();
    
    System.out.println("List of requests: "+RequestList);  
    }
}