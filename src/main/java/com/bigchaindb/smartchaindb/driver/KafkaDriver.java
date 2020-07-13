package com.bigchaindb.smartchaindb.driver;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.core.pattern.AbstractStyleNameConverter.Red;

public class KafkaDriver {

	String req;
	Producer<String, String> producer;

	public KafkaDriver(String req) {
		this.req = req;
	}

	public void runProducer(String topic) {
		if (producer == null) {
			Random rand = new Random();
			producer = ProducerCreator.createRequestProducer("requestor" + rand.nextInt(10));
		}

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, req);
		try {
			RecordMetadata metadata = producer.send(record).get();

			System.out.println("Record sent to topic " + topic + " at partition " + metadata.partition()
					+ " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}
	}
}
