package com.bigchaindb.smartchaindb.driver;

import java.util.*;

public class ConsumerDriver {
    public static void main(final String[] args) throws InterruptedException {
        final int threadCount = 1;
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

            KafkaConsumerGroup group = new KafkaConsumerGroup(randomTopics, i);

            // final Thread thread = new Thread(new ParallelConsumers(randomTopics, i + 1),
            // "Thread-" + (i + 1));
            // System.out.println("\nThread" + (i + 1) + " subscribed to topics: " +
            // randomTopics.toString());
            // Thread.sleep(1000);
            // thread.start();
        }
    }
}