package com.bigchaindb.smartchaindb.driver;

public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "152.7.99.26:9092";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 300;
    public static String OFFSET_RESET_LATEST = "latest";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static Integer MAX_POLL_RECORDS = 1;
}