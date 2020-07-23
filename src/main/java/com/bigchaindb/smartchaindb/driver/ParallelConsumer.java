package com.bigchaindb.smartchaindb.driver;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONArray;
import org.json.JSONObject;

public class ParallelConsumer implements Runnable {

    private KafkaConsumerGroup manager;
    private final String topic;
    private final Consumer<String, String> consumer;

    public ParallelConsumer(KafkaConsumerGroup manager, String topic, int consumerRank) {
        this.manager = manager;
        this.topic = topic;
        String consumerGroup = "consumerGroup-" + this.manager.getRank() + "-" + consumerRank + LocalDateTime.now();
        consumer = ConsumerCreator.createRequestConsumer(consumerGroup);
    }

    @Override
    public void run() {

        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(Thread.currentThread().getName() + "-" + this.topic + ".csv", true))) {

            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<String, String> consumerRecords = consumer
                        .poll(Duration.ofMillis(Long.MAX_VALUE));
                Map<String, String> conditionMap = manager.getTopicConditionMap().get(topic);

                consumerRecords.forEach(record -> {
                    System.out.println("\nRecord: " + record.value());
                    final JSONObject jsonReq = new JSONObject(record.value());

                    if (conditionMap == null || checkConditions(jsonReq, conditionMap)) {
                        checkRequest(jsonReq);
                        writeToLog(writer, jsonReq);
                    }
                });

                consumer.commitAsync();
            }
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private boolean checkConditions(final JSONObject jsonReq, Map<String, String> conditionMap) {
        boolean result = true;

        /*
         * for (String key : conditionMap.keySet()) { JSONArray jsonArray =
         * jsonReq.getJSONArray("products"); Gson gson = new Gson(); Type type = new
         * TypeToken<Map<String, String>>() { }.getType();
         *
         * for (int i = 0; i < jsonArray.length(); i++) { JSONObject currentObject =
         * jsonArray.getJSONObject(i); Map<String, String> productMetadata =
         * gson.fromJson(currentObject.toString(), type); } }
         */

        return result;
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

    private void checkRequest(final JSONObject jsonReq) {

        if (jsonReq.has("Capability") && jsonReq.has("Transaction_id")) {
            boolean matchFound = true;
            final JSONArray reqCapabilities = jsonReq.getJSONArray("Capability");
            final Set<String> offeredCapabilities = manager.getSubscribedTopics();

            for (int i = 0; i < reqCapabilities.length(); i++) {
                if (!offeredCapabilities.contains(reqCapabilities.get(i))) {
                    matchFound = false;
                    break;
                }
            }

            if (matchFound) {
                // RequestList.add(jsonReq);
                System.out.println(
                        "\n\n************************************ Match Found ************************************");
                System.out.println("Transaction Id: " + jsonReq.getString("Transaction_id"));
                System.out.println(
                        "******************************************************************************************\n\n");
            }
        }

    }
}
