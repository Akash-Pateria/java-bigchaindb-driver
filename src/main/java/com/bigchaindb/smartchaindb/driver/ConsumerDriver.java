package com.bigchaindb.smartchaindb.driver;

import java.util.*;

public class ConsumerDriver {
    public static void main(final String[] args) throws InterruptedException {
        final int maxCapabilityCount = 8;
        final Random random = new Random();

        final List<String> allTopics = new ArrayList<String>(Capabilities.getAll());
        final List<String> randomTopics = new ArrayList<>();
        randomTopics.add(Capabilities.MISC);

        for (int k = 0; k < maxCapabilityCount; k++) {
            final int randomIndex = random.nextInt(allTopics.size());
            randomTopics.add(allTopics.get(randomIndex));
            allTopics.remove(randomIndex);
        }

        KafkaConsumerGroup group = new KafkaConsumerGroup(randomTopics, 0);
    }
}