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
    public static void main(final String[] args) throws InterruptedException {
        final int threadCount = 2;
        final int maxCapabilityCount = 3;
        final Random random = new Random();

        for (int i = 0; i < threadCount; i++) {
            final List<String> allTopics = new ArrayList<String>(Capabilities.getAll());
            final List<String> randomTopics = new ArrayList<>();
            randomTopics.add(Capabilities.MISC);

            for (int k = 0; k < maxCapabilityCount; k++) {
                final int randomIndex = random.nextInt(allTopics.size());
                randomTopics.add(allTopics.get(randomIndex));
                allTopics.remove(randomIndex);
            }

            final Thread thread = new Thread(new ParallelConsumers(randomTopics, i + 1), "Thread-" + (i + 1));
            System.out.println("\nThread" + (i + 1) + " subscribed to topics: " + randomTopics.toString());
            Thread.sleep(1000);
            thread.start();
        }
    }

    private static class ParallelConsumers implements Runnable {
        private final List<JSONObject> RequestList;
        private final HashSet<String> processedRequests;
        private final Set<String> subscribedTopics;
        private final int consumerRank;
        private final Consumer<String, String> consumer;

        ParallelConsumers(final List<String> topics, final int consumerNum) {
            RequestList = new ArrayList<>();
            processedRequests = new HashSet<>();
            subscribedTopics = new HashSet<>(topics);
            consumerRank = consumerNum;
            consumer = ConsumerCreator
                    .createRequestConsumer("consumerGroup-" + consumerRank + "-" + LocalDateTime.now());
        }

        @Override
        public void run() {

            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter(Thread.currentThread().getName() + ".csv", true))) {

                final AtomicBoolean addRequest = new AtomicBoolean(true);
                consumer.subscribe(subscribedTopics);

                while (true) {
                    final ConsumerRecords<String, String> consumerRecords = consumer
                            .poll(Duration.ofMillis(Long.MAX_VALUE));
                    /*
                     * if (consumerRecords.count() == 0) { noMessageFound++; if (noMessageFound >
                     * IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) { break; } else { continue; } }
                     */

                    consumerRecords.forEach(record -> {
                        System.out.println("\nRecord: " + record.value());
                        final JSONObject jsonReq = new JSONObject(record.value());

                        if (!processedRequests.contains(jsonReq.get("Transaction_id"))) {
                            checkRequest(subscribedTopics, addRequest, jsonReq);
                        }

                        writeToLog(writer, jsonReq);
                    });

                    consumer.commitAsync();
                }
            } catch (final Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }

        private void writeToLog(BufferedWriter writer, final JSONObject jsonReq) {
            LocalDateTime creationDateTime = jsonReq.has("requestCreationTimestamp")
                    ? LocalDateTime.parse(jsonReq.getString("requestCreationTimestamp"))
                    : LocalDateTime.now();
            LocalDateTime kafkaInTimestamp = jsonReq.has("kafkaInTimestamp")
                    ? LocalDateTime.parse(jsonReq.getString("kafkaInTimestamp"))
                    : LocalDateTime.now();
            LocalDateTime now = LocalDateTime.now();

            final long timeDifferenceInMillis1 = Duration.between(creationDateTime, now).toMillis();
            final long timeDifferenceInMillis2 = Duration.between(kafkaInTimestamp, now).toMillis();

            int productCount = jsonReq.has("products") ? jsonReq.getJSONArray("products").length() : 1;
            int capabilityCount = jsonReq.has("Capability") ? jsonReq.getJSONArray("Capability").length() : 1;

            try {
                writer.write(creationDateTime + "," + kafkaInTimestamp + "," + now + "," + timeDifferenceInMillis1 + ","
                        + timeDifferenceInMillis2 + "," + productCount + "," + capabilityCount + "\n");
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }

        private void checkRequest(Set<String> checkTopics, final AtomicBoolean addRequest, final JSONObject jsonReq) {
            if (jsonReq.has("Capability") && jsonReq.has("Transaction_id")) {
                final JSONArray reqCapabilities = jsonReq.getJSONArray("Capability");
                boolean matchFound = true;

                for (int i = 0; i < reqCapabilities.length(); i++) {
                    if (!checkTopics.contains(reqCapabilities.get(i))) {
                        addRequest.set(false);
                        matchFound = false;
                        break;
                    }
                }

                final boolean flag = addRequest.get();
                if (matchFound == true) {
                    processedRequests.add((String) jsonReq.get("Transaction_id"));
                    RequestList.add(jsonReq);
                    System.out.println(
                            "\n\n************************************ Match Found ************************************");
                    System.out.println("Transaction Id: " + jsonReq.get("Transaction_id"));
                    System.out.println(
                            "******************************************************************************************\n\n");
                }
            }

        }
    }
}