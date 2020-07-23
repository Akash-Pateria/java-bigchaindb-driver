package com.bigchaindb.smartchaindb.driver;

import java.util.*;

import org.apache.mina.util.ConcurrentHashSet;
import org.json.JSONObject;

public class KafkaConsumerGroup {
    protected Map<String, Thread> topicConsumerThread;
    protected Map<String, Map<String, String>> topicConditionMap;
    protected int rank;
    // protected Set<String> subscribedTopics;
    // protected List<JSONObject> RequestList;
    // protected Set<String> processedTransactionIds;

    KafkaConsumerGroup() {
        topicConsumerThread = new HashMap<>();
        topicConditionMap = new HashMap<>();
        rank = 0;
        // subscribedTopics = new HashSet<>();
        // RequestList = new ArrayList<>();
        // processedTransactionIds = new ConcurrentHashSet<>();
    }

    public KafkaConsumerGroup(List<String> topics, int managerRank) {
        this();
        this.rank = managerRank;
        subscribe(topics);
        // subscribedTopics.addAll(topics);
    }

    public void addTopic(List<String> newTopics) {
        // subscribedTopics.add(newTopic);
        subscribe(newTopics);
    }

    public void unsubscribeTopics(List<String> topics) {
        // subscribedTopics.add(newTopic);
        for (int i = 0; i < topics.size(); i++) {
            String topic = topics.get(i);
            Thread consumerThread = topicConsumerThread.get(topic);

            consumerThread.interrupt();
            topicConsumerThread.remove(topic);
        }
    }

    public void addConditions(String topic, String conditionKey, String conditionValue) {
        Map<String, String> conditionMap = topicConditionMap.get(topic);
        conditionMap.put(conditionKey, conditionValue);
    }

    public void addConditions(String topic, Map<String, String> conditions) {
        Map<String, String> conditionMap = topicConditionMap.get(topic);
        conditionMap.putAll(conditions);
    }

    public Set<String> getSubscribedTopics() {
        // return subscribedTopics;
        return topicConsumerThread.keySet();
    }

    public Map<String, Map<String, String>> getTopicConditionMap() {
        return topicConditionMap;
    }

    public void setTopicConditionMap(Map<String, Map<String, String>> topicConditionMap) {
        this.topicConditionMap = topicConditionMap;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    private void subscribe(List<String> topics)
    {
        for (int i = 0; i < topics.size(); i++) {
            String topic = topics.get(i);
            ParallelConsumer consumer = new ParallelConsumer(this, topic, i);

            final Thread thread = new Thread(consumer, "Consumer-" + rank + "-" + i);
            topicConsumerThread.put(topic, thread);
            System.out.println("\nThread" + thread.getName() + " subscribed to topic: " + topic);
            thread.start();
        }
    }
}
