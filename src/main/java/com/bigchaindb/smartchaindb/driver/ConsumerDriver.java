package com.bigchaindb.smartchaindb.driver;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

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
            allTopics.add(Capabilities.MISC);

            final List<String> randomTopics = new ArrayList<>();
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
        private final Set<String> subscribedTopics;
        private final int consumerRank;
        private final Consumer<String, String> consumer;

        ParallelConsumers(final List<String> topics, final int consumerNum) {
            RequestList = new ArrayList<>();
            subscribedTopics = new HashSet<String>(topics);
            consumerRank = consumerNum;
            consumer = ConsumerCreator
                    .createRequestConsumer("consumerGroup-" + consumerRank + "-" + LocalDateTime.now());
        }

        @Override
        public void run() {

            try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter(Thread.currentThread().getName() + ".txt", true))) {

                final AtomicBoolean addRequest = new AtomicBoolean(true);
                consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));

                while (true) {
                    final ConsumerRecords<String, String> consumerRecords = consumer
                            .poll(Duration.ofMillis(Long.MAX_VALUE));

                    consumerRecords.forEach(record -> {
                        System.out.println("\nRecord: " + record.value());
                        final JSONObject jsonReq = new JSONObject(record.value());

                        int capabilityCount = checkRequest(subscribedTopics, addRequest, jsonReq);
                        writeToLog(writer, jsonReq, capabilityCount);
                    });

                    consumer.commitAsync();
                }
            } catch (final Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }

        private void writeToLog(BufferedWriter writer, final JSONObject jsonReq, int capabilityCount) {

            LocalDateTime creationDateTime = jsonReq.has("requestCreationTimestamp")
                    ? LocalDateTime.parse(jsonReq.getString("requestCreationTimestamp"))
                    : LocalDateTime.now();

            LocalDateTime kafkaInTimestamp = jsonReq.has("kafkaInTimestamp")
                    ? LocalDateTime.parse(jsonReq.getString("kafkaInTimestamp"))
                    : LocalDateTime.now();

            LocalDateTime now = LocalDateTime.now();
            int productCount = jsonReq.has("productCount") ? jsonReq.getInt("productCount") : 1;

            final long timeDifferenceInMillis1 = Duration.between(creationDateTime, now).toMillis();
            final long timeDifferenceInMillis2 = Duration.between(kafkaInTimestamp, now).toMillis();

            try {
                writer.write(creationDateTime + "," + kafkaInTimestamp + "," + now + "," + timeDifferenceInMillis1 + ","
                        + timeDifferenceInMillis2 + "," + productCount + "," + capabilityCount + "\n");
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }

        private int checkRequest(Set<String> checkTopics, final AtomicBoolean addRequest, final JSONObject jsonReq) {

            int capabilityCount = 0;
            if (jsonReq.has("products") && jsonReq.has("Transaction_id")) {

                List<String> reqCapabilities = inferCapabilities(jsonReq);
                capabilityCount = reqCapabilities.size();
                boolean bin = true;

                for (int i = 0; i < capabilityCount; i++) {
                    if (!checkTopics.contains(reqCapabilities.get(i))) {
                        addRequest.set(false);
                        bin = false;
                        break;
                    }
                }

                final boolean flag = addRequest.get();
                if (bin == true) {
                    RequestList.add(jsonReq);
                    System.out.println("\n\n****************** Match Found ******************");
                    System.out.println("Transaction Id: " + jsonReq.get("Transaction_id"));
                    System.out.println("******************************************************\n\n");
                }
            }

            return capabilityCount;
        }

        private List<String> inferCapabilities(final JSONObject jsonReq) {

            Set<String> allCapability = new HashSet<String>();
            JSONArray jsonArray = jsonReq.getJSONArray("products");
            Gson gson = new Gson();
            Type type = new TypeToken<Map<String, String>>() {
            }.getType();

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject currentObject = jsonArray.getJSONObject(i);
                Map<String, String> productMetadata = gson.fromJson(currentObject.toString(), type);

                List<String> attributes = new ArrayList<>(productMetadata.keySet());
                List<String> capability = RulesDriver.getCapabilities(attributes, productMetadata);
                allCapability.addAll(capability);
            }
            System.out.println("Inferred Capabilities: " + allCapability.toString());
            return new ArrayList<>(allCapability);
        }
    }
}