package org.sn.myutils.pubsub;

public enum ReceiveMode {
    /**
     * A subscriber will receive all messages published to the central server for the topic.
     */
    PUBSUB,

    /**
     * Only one queue subscriber will receive a message published to the central server for the topic.
     * Each message published will be sent to a different subscriber.
     */
    QUEUE
}
