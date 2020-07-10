package com.bigchaindb.smartchaindb.driver;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONArray;
import org.json.JSONObject;

public class ConsumerDriver {
    public static void main(String[] args) throws InterruptedException {
        int threadCount = 2;
        int maxCapabilityCount = 3;
        Random random = new Random();
        List<String> allTopics = new ArrayList<String>(Capabilities.getAll());
        allTopics.add(Capabilities.MISC);

        for (int i = 0; i < threadCount; i++) {
            List<String> randomTopics = new ArrayList<>();
            for (int k = 0; k < maxCapabilityCount; k++) {
                int randomIndex = random.nextInt(allTopics.size());
                randomTopics.add(allTopics.get(randomIndex));
            }

            Thread thread = new Thread(new ParallelConsumers(randomTopics, i + 1), "Thread" + (i + 1));
            System.out.println("\nThread" + (i + 1) + " subscribed to topics: " + randomTopics.toString());
            Thread.sleep(5000);
            thread.start();
        }
    }

    private static class ParallelConsumers implements Runnable {
        private List<String> subscribedTopics;
        private int consumerRank;

        ParallelConsumers(List<String> topics, int consumerNum) {
            subscribedTopics = topics;
            consumerRank = consumerNum;
        }

        @Override
        public void run() {
            Consumer<String, String> consumer = ConsumerCreator.createRequestConsumer("consumerGroup" + consumerRank);
            HashSet<String> checkTopics = new HashSet<String>(subscribedTopics);
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(Thread.currentThread().getName()));

                consumer.subscribe(subscribedTopics);

                // hash set to store the transaction id it has already check.
                HashSet<String> checkRequest = new HashSet<>();
                AtomicBoolean addRequest = new AtomicBoolean(true);
                // List to store all the request it has completely matched with its
                // capabilities.
                List<JSONObject> RequestList = new ArrayList<>();

                int noMessageFound = 0;
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                    // 1000 is the time in milliseconds consumer will wait if no record is found at
                    // broker.
                    if (consumerRecords.count() == 0) {
                        noMessageFound++;
                        if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
                            // If no message found count is reached to threshold exit loop.
                            break;
                        } else
                            continue;
                    }

                    // print each record.
                    consumerRecords.forEach(record -> {
                        System.out.println("Record Key " + record.key());
                        System.out.println("Record value " + record.value());
                        System.out.println("Record partition " + record.partition());
                        System.out.println("Record offset " + record.offset());
                        JSONObject jsonReq = new JSONObject(record.value());

                        LocalDateTime creationDateTime = LocalDateTime.parse(jsonReq.get("timestamp").toString());
                        LocalDateTime now = LocalDateTime.now();
                        long timeDifferenceInMillis = Duration.between(creationDateTime, now).toMillis();

                        try {
                            writer.write(creationDateTime + "," + now + "," + timeDifferenceInMillis);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        // check the transaction id is already present in checkRequest, if so drop or
                        // check the capabilities
                        if (!checkRequest.contains(jsonReq.get("Transaction_id"))) {
                            // check all topics are present in checktopics
                            JSONArray reqCapabilities = jsonReq.getJSONArray("Capability");

                            for (int i = 0; i < reqCapabilities.length(); i++) {
                                if (!checkTopics.contains(reqCapabilities.get(i))) {
                                    addRequest.set(false);
                                    break;
                                }
                            }
                            boolean value = addRequest.get();
                            if (value == true) {
                                // add in requestList and add the request in checkRequest
                                checkRequest.add((String) jsonReq.get("Transaction_id"));
                                RequestList.add(jsonReq);
                            }
                        }
                    });

                    // commits the offset of record to broker.
                    consumer.commitAsync();
                }

                writer.close();
                // System.out.println("\n\n\n*****[" + Thread.currentThread().getName() + "]List
                // of requests: \n"
                // + RequestList.toString());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }
    }

    /*
     * static void runConsumer() { // function to create a consumer group which
     * polls at a interval of time and // check the record with its capabilities and
     * if matched it simply adds in to // RequestList. Consumer<String, String>
     * consumer = ConsumerCreator.createRequestConsumer("consumerGroup1");
     * List<String> topics = Arrays.asList(Capabilities.PLASTIC,
     * Capabilities.MILLING); HashSet<String> checkTopics = new HashSet<String>();
     * 
     * consumer.subscribe(topics);
     * 
     * // hash set to store the transaction id it has already check. HashSet<String>
     * checkRequest = new HashSet<>(); AtomicBoolean addRequest = new
     * AtomicBoolean(true); // List to store all the request it has completely
     * matched with its // capabilities. List<JSONObject> RequestList = new
     * ArrayList<>();
     * 
     * int noMessageFound = 0; while (true) { ConsumerRecords<String, String>
     * consumerRecords = consumer.poll(Duration.ofMillis(1000)); // 1000 is the time
     * in milliseconds consumer will wait if no record is found at // broker. if
     * (consumerRecords.count() == 0) { noMessageFound++; if (noMessageFound >
     * IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) { // If no message found count is
     * reached to threshold exit loop. break; } else continue; }
     * 
     * // print each record. consumerRecords.forEach(record -> {
     * System.out.println("Record Key " + record.key());
     * System.out.println("Record value " + record.value());
     * System.out.println("Record partition " + record.partition());
     * System.out.println("Record offset " + record.offset()); JSONObject jsonReq =
     * new JSONObject(record.value());
     * 
     * // check the transaction id is already present in checkRequest, if so drop or
     * // check the capabilities if
     * (!checkRequest.contains(jsonReq.get("Transaction_id"))) { // check all topics
     * are present in checktopics JSONArray reqCapabilities =
     * jsonReq.getJSONArray("Capability");
     * 
     * // System.out.println("Id:" + jsonReq.get("Transaction_id") ); for (int i =
     * 0; i < reqCapabilities.length(); i++) { if
     * (!checkTopics.contains(reqCapabilities.get(i))) { addRequest.set(false);
     * break; } } boolean value = addRequest.get(); if (value == true) { // add in
     * requestList and add the request in checkRequest checkRequest.add((String)
     * jsonReq.get("Transaction_id")); RequestList.add(jsonReq); } } }); // commits
     * the offset of record to broker. consumer.commitAsync(); } consumer.close();
     * 
     * System.out.println("List of requests: " + RequestList); }
     */
}