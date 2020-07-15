package com.bigchaindb.smartchaindb.driver;

import java.util.*;

import org.apache.mina.util.ConcurrentHashSet;
import org.json.JSONObject;

public class KafkaConsumerGroup {
    protected Set<String> subscribedTopics;
    protected Map<String, ParallelConsumer> topicConsumerMap;
    // protected List<JSONObject> RequestList;
    protected Set<String> matchedTransactionIds;
    protected Map<String, Map<String, String>> topicConditionMap;
    protected int managerRank;

    KafkaConsumerGroup() {
        subscribedTopics = new HashSet<>();
        topicConsumerMap = new HashMap<>();
        // RequestList = new ArrayList<>();
        matchedTransactionIds = new ConcurrentHashSet<>();
        topicConditionMap = new HashMap<>();
        managerRank = 0;
    }

    public KafkaConsumerGroup(List<String> topics, int managerRank) {
        this();
        subscribedTopics.addAll(topics);
        this.managerRank = managerRank;
        for (int i = 0; i < topics.size(); i++) {
            String topic = topics.get(i);
            ParallelConsumer consumer = new ParallelConsumer(topic, i);
            topicConsumerMap.put(topic, consumer);

            final Thread thread = new Thread(consumer, "Consumer-" + managerRank + "-" + i);
            System.out.println("\nThread" + thread.getName() + " subscribed to topic: " + topic);
            thread.start();
        }
    }

    public void addTopic(String newTopic) {
        subscribedTopics.add(newTopic);
    }

    public void addConditions(String topic, String conditionKey, String conditionValue) {
        Map<String, String> conditionMap = topicConditionMap.get(topic);
        conditionMap.put(conditionKey, conditionValue);
    }

    public void addConditions(String topic, Map<String, String> conditions) {
        Map<String, String> conditionMap = topicConditionMap.get(topic);
        conditionMap.putAll(conditions);
    }
}
