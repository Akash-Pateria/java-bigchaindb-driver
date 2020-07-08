package com.bigchaindb.smartchaindb.driver;

public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "152.7.99.26:9092";
    public static Integer MESSAGE_COUNT = 5;
    public static String CLIENT_ID = "client1";
    public static String TOPIC_NAME = "requests";
    public static String GROUP_ID_CONFIG = "consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 10;
    public static String OFFSET_RESET_LATEST = "latest";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static Integer MAX_POLL_RECORDS = 1;
}