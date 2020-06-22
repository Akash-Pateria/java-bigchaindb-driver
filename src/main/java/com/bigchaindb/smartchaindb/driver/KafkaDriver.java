package com.bigchaindb.smartchaindb.driver;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//import com.gaurav.kafka.constants.IKafkaConstants;
//import com.gaurav.kafka.consumer.ConsumerCreator;
//import com.gaurav.kafka.producer.ProducerCreator;
public class KafkaDriver {

	String req;

	public KafkaDriver(String req) {
		this.req = req;

	}

	public void runProducer(String topic) {
		Producer<String, String> producer = ProducerCreator.createRequestProducer();

		System.out.println("Final request: " + req);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, req);
		try {
			RecordMetadata metadata = producer.send(record).get();

			System.out
					.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}
	}
}
