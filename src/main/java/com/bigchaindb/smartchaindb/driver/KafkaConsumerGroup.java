package com.bigchaindb.smartchaindb.driver;

import java.util.*;

import org.apache.mina.util.ConcurrentHashSet;
import org.json.JSONObject;

public class KafkaConsumerGroup {
    protected Set<String> subscribedTopics;
    // protected List<JSONObject> RequestList;
    // protected Set<String> processedTransactionIds;
    protected Map<String, Map<String, String>> topicConditionMap;
    protected int rank;

    KafkaConsumerGroup() {
        subscribedTopics = new HashSet<>();
        // RequestList = new ArrayList<>();
        // processedTransactionIds = new ConcurrentHashSet<>();
        topicConditionMap = new HashMap<>();
        rank = 0;
    }

    public KafkaConsumerGroup(List<String> topics, int managerRank) {
        this();
        subscribedTopics.addAll(topics);
        this.rank = managerRank;
        for (int i = 0; i < topics.size(); i++) {
            String topic = topics.get(i);
            ParallelConsumer consumer = new ParallelConsumer(this, topic, i);

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

    public Set<String> getSubscribedTopics() {
        return subscribedTopics;
    }

    public void setSubscribedTopics(Set<String> subscribedTopics) {
        this.subscribedTopics = subscribedTopics;
    }

    // public Set<String> getProcessedTransactionIds() {
    // return processedTransactionIds;
    // }

    // public void addProcessedTransactionIds(String transactionId) {
    // this.processedTransactionIds.add(transactionId);
    // }

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
}
