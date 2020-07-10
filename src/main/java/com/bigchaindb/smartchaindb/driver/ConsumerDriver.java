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
        final List<String> allTopics = new ArrayList<String>(Capabilities.getAll());
        allTopics.add(Capabilities.MISC);

        for (int i = 0; i < threadCount; i++) {
            final List<String> randomTopics = new ArrayList<>();
            for (int k = 0; k < maxCapabilityCount; k++) {
                final int randomIndex = random.nextInt(allTopics.size());
                randomTopics.add(allTopics.get(randomIndex));
            }

            final Thread thread = new Thread(new ParallelConsumers(randomTopics, i + 1), "Thread-" + (i + 1));
            System.out.println("\nThread" + (i + 1) + " subscribed to topics: " + randomTopics.toString());
            Thread.sleep(5000);
            thread.start();
        }
    }

    private static class ParallelConsumers implements Runnable {
        private final List<JSONObject> RequestList;
        private final HashSet<String> processedRequests;
        private final List<String> subscribedTopics;
        private final int consumerRank;
        private final Consumer<String, String> consumer;

        ParallelConsumers(final List<String> topics, final int consumerNum) {
            RequestList = new ArrayList<>();
            processedRequests = new HashSet<>();
            subscribedTopics = topics;
            consumerRank = consumerNum;
            consumer = ConsumerCreator
                    .createRequestConsumer("consumerGroup-" + consumerRank + "-" + LocalDateTime.now());
        }

        @Override
        public void run() {
            int noMessageFound = 0;
            final HashSet<String> checkTopics = new HashSet<String>(subscribedTopics);

            try {
                final AtomicBoolean addRequest = new AtomicBoolean(true);
                BufferedWriter writer = new BufferedWriter(
                        new FileWriter(Thread.currentThread().getName() + ".txt", true));

                consumer.subscribe(subscribedTopics);

                while (true) {
                    final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                    if (consumerRecords.count() == 0) {
                        noMessageFound++;
                        if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
                            break;
                        } else {
                            continue;
                        }
                    }

                    consumerRecords.forEach(record -> {
                        System.out.println("\nRecord: " + record.value());
                        final JSONObject jsonReq = new JSONObject(record.value());

                        if (!processedRequests.contains(jsonReq.get("Transaction_id"))) {
                            checkRequest(checkTopics, addRequest, jsonReq);
                        }

                        writeToLog(writer, jsonReq);
                    });

                    consumer.commitAsync();
                }

                writer.close();
            } catch (final Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }

        private void writeToLog(BufferedWriter writer, final JSONObject jsonReq) {
            LocalDateTime now = LocalDateTime.now(), creationDateTime = LocalDateTime.now(),
                    kafkaInTimestamp = LocalDateTime.now();

            if (jsonReq.has("requestCreationTimestamp")) {
                creationDateTime = LocalDateTime.parse(jsonReq.getString("requestCreationTimestamp"));
            }

            if (jsonReq.has("kafkaInTimestamp")) {
                kafkaInTimestamp = LocalDateTime.parse(jsonReq.getString("kafkaInTimestamp"));
            }

            final long timeDifferenceInMillis = Duration.between(creationDateTime, now).toMillis();
            try {
                writer.write(
                        creationDateTime + "," + kafkaInTimestamp + "," + now + "," + timeDifferenceInMillis + "\n");
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }

        private void checkRequest(final HashSet<String> checkTopics, final AtomicBoolean addRequest,
                final JSONObject jsonReq) {
            if (jsonReq.has("Capability") && jsonReq.has("Transaction_id")) {
                final JSONArray reqCapabilities = jsonReq.getJSONArray("Capability");

                for (int i = 0; i < reqCapabilities.length(); i++) {
                    if (!checkTopics.contains(reqCapabilities.get(i))) {
                        addRequest.set(false);
                        break;
                    }
                }

                final boolean flag = addRequest.get();
                if (flag == true) {
                    processedRequests.add((String) jsonReq.get("Transaction_id"));
                    RequestList.add(jsonReq);
                }
            }

        }
    }
}