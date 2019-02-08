package com.bigchaindb.smartchaindb.driver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;

//import com.gaurav.kafka.constants.IKafkaConstants;
//import com.gaurav.kafka.consumer.ConsumerCreator;
//import com.gaurav.kafka.producer.ProducerCreator;
public class KafkaDriver {
	
	String req;
	
    public KafkaDriver(String req) {
       	this.req = req;
    }
    
    public void runConsumer() { 	
        Consumer<String, String> consumer = ConsumerCreator.createRequestConsumer();
        
        List<JSONObject> RequestList = new ArrayList<JSONObject>();
        int noMessageFound = 0;
        while (true) {
        	ConsumerRecords<String, String> consumerRecords = consumer.poll(10);
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
    public void runProducer(String topic) {
    	Producer<String, String> producer = ProducerCreator.createRequestProducer();
    	
    	System.out.println("Final request: "+req);
    	
    	ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, req);
    	try {
    		RecordMetadata metadata = producer.send(record).get();

            System.out.println("Record sent to partition " + metadata.partition()+ " with offset " + metadata.offset());
    	} 
    	catch (ExecutionException e) {
    		System.out.println("Error in sending record");
    		System.out.println(e);
    	} 
    	catch (InterruptedException e) {
    		System.out.println("Error in sending record");
    		System.out.println(e);
    	}
    }
}





