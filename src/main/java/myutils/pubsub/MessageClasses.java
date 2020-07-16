package myutils.pubsub;

import java.io.Serializable;
import javax.annotation.Nonnull;


/**
 * All of the messages sent between client and server.
 */
interface MessageClasses {
    
    /**
     * This base class of all messages sent between client and server.
     */
    interface MessageBase extends Serializable {
    }
    
    
    //////////////////////////////////////////////////////////////////////
    // Server generated messages

    /**
     * Messages originating from server and sent to client.
     * Required field serverTimestamp, which is the time the server generated the message.
     */
    abstract class ServerGeneratedMessage implements MessageBase {
        private static final long serialVersionUID = 1L;

        private long serverTimestamp;

        ServerGeneratedMessage() {
            this.serverTimestamp = System.currentTimeMillis();
        }

        long getServerTimestamp() {
            return serverTimestamp;
        }
    }

    /**
     * Class sent by server to client to request identification.
     * Sent when server receives a command from a machine it does not know about.
     * Required field failedClientIndex, which identifies the message which failed, indicating to the client that it should resend this message;
     */
    class RequestIdentification extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final Class<? extends MessageBase> classOfMessageToResend;
        private final Long failedClientIndex; // optional because not all messages a clientIndex
        
        RequestIdentification(Class<? extends MessageBase> classOfMessageToResend, Long failedClientIndex) {
            this.classOfMessageToResend = classOfMessageToResend;
            this.failedClientIndex = failedClientIndex;
        }
        
        Class<? extends MessageBase> getClassOfMessageToResend() {
            return classOfMessageToResend;
        }
        
        long getFailedClientIndex() {
            return failedClientIndex;
        }
    }
    
    class InvalidRelayMessage extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final long failedClientIndex; // optional because not all messages a clientIndex
        private final String error;

        InvalidRelayMessage(long failedClientIndex, @Nonnull String error) {
            this.failedClientIndex = failedClientIndex;
            this.error = error;
        }

        public long getFailedClientIndex() {
            return failedClientIndex;
        }

        public String getError() {
            return error;
        }
    }

    
    //////////////////////////////////////////////////////////////////////
    // Client generated messages
    
    /**
     * Messages originating from client and sent to server, which they may be relayed to other clients.
     * Required field clientTimestamp, which is the time the client sent the message.
     * Required field serverTimestamp, which is the time the server received the message, or 0 if the message is yet to be sent to the server.
     */
    abstract class ClientGeneratedMessage implements MessageBase {
        private static final long serialVersionUID = 1L;
        
        private long clientTimestamp;

        ClientGeneratedMessage() {
            this.clientTimestamp = System.currentTimeMillis();
        }

        long getClientTimestamp() {
            return clientTimestamp;
        }
    }

    /**
     * Class sent by client to register itself to the server.
     * Sent when client first connects to server.
     * Required field machineId, which must be unique across all machines.
     */
    class Identification extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final String machineId;
        
        Identification(String machineId) {
            this.machineId = machineId;
        }
        
        String getMachineId() {
            return machineId;
        }
    }

    /**
     * Class to notify the server that we are subscribing to or unsubscribing from a particular topic,
     * so that it sends or stops sending us messages published to this topic.
     * Required fields topic and subscriberName.
     */
    abstract class AddOrRemoveSubscriber extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private String topic;
        private String subscriberName;

        public AddOrRemoveSubscriber(String topic, String subscriberName) {
            this.topic = topic;
            this.subscriberName = subscriberName;
        }
        
        String getTopic() {
            return topic;
        }
        
        String getSubscriberName() {
            return subscriberName;
        }
    }
    
    /**
     * Class to notify the server that we are subscribing to a particular topic.
     * Required field shouldTryDownload.
     */
    class AddSubscriber extends AddOrRemoveSubscriber {
        private static final long serialVersionUID = 1L;
        
        private boolean tryDownload;
        
        public AddSubscriber(String topic, String subscriberName, boolean tryDownload) {
            super(topic, subscriberName);
            this.tryDownload = tryDownload;
        }

        public boolean shouldTryDownload() {
            return tryDownload;
        }
    }
    
    /**
     * Class to notify the server that we are unsubscribing from a particular topic.
     * No new required fields.
     */
    class RemoveSubscriber extends AddOrRemoveSubscriber {
        private static final long serialVersionUID = 1L;
        
        public RemoveSubscriber(String topic, String subscriberName) {
            super(topic, subscriberName);
        }
    }
    
    /**
     * Class sent by client to download published messages even if they have already been sent to the client.
     * The server will send a PublishMessage for each object that it has in its cache.
     */
    class DownloadPublishedMessages extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private long startServerIndexInclusive;
        private long endServerIndexInclusive;

        public DownloadPublishedMessages(long startServerIndexInclusive, long endServerIndexInclusive) {
            this.startServerIndexInclusive = startServerIndexInclusive;
            this.endServerIndexInclusive = endServerIndexInclusive;
        }
        
        long getStartServerIndexInclusive() {
            return startServerIndexInclusive;
        }
        
        long getEndServerIndexInclusive() {
            return endServerIndexInclusive;
        }
    }
    
    /**
     * Base class of all messages that can be relayed from one client to another via the server.
     * Required field sourceMachineId, which is the machine sending the message. The field is set by server.
     * Required field clientIndex, which is the message number on the client.
     * Required field serverIndex, which is the message number on the server, or 0 if the message is yet to be sent to the server.
     * It is unique across messages in all machines. 
     */
    abstract class RelayMessageBase extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;

        private long serverTimestamp;
        private String sourceMachineId;
        private long clientIndex;
        private long serverIndex; // 0 if message has not yet been sent to server
        
        RelayMessageBase() {
        }
        
        RelayMessageBase(long clientIndex) {
            this.clientIndex = clientIndex;
        }
        
        void setServerTimestampAndSourceMachineIdAndIndex(String sourceMachineId, long serverIndex) {
            this.serverTimestamp = System.currentTimeMillis();
            this.sourceMachineId = sourceMachineId;
            this.serverIndex = serverIndex;
        }
        
        long getServerTimestamp() {
            return serverTimestamp;
        }
        
        String getSourceMachineId() {
            return sourceMachineId;
        }

        long getClientIndex() {
            return clientIndex;
        }

        long getServerIndex() {
            return serverIndex;
        }
    }
    
    /**
     * Relay messages with a topic.
     */
    abstract class RelayTopicMessageBase extends RelayMessageBase {
        private static final long serialVersionUID = 1L;

        private final @Nonnull String topic;
        
        RelayTopicMessageBase(long clientIndex, @Nonnull String topic) {
            super(clientIndex);
            this.topic = topic;
        }
        
        String getTopic() {
            return topic;
        }
    }
    
    /**
     * Action representing the createPublisher command.
     */
    class CreatePublisher extends RelayTopicMessageBase {
        private static final long serialVersionUID = 1L;
        
        private final @Nonnull Class<?> publisherClass;

        CreatePublisher(long clientIndex, @Nonnull String topic, @Nonnull Class<?> publisherClass) {
            super(clientIndex, topic);
            this.publisherClass = publisherClass;
        }

        Class<?> getPublisherClass() {
            return publisherClass;
        }
    }
    
    
    /**
     * Action representing the publisher.publish command.
     */
    class PublishMessage extends RelayTopicMessageBase {
        private static final long serialVersionUID = 1L;
        
        private final @Nonnull String topic;
        private final @Nonnull CloneableObject<?> message;
        private final @Nonnull MessagePriority priority;
        
        PublishMessage(long clientIndex, @Nonnull String topic, @Nonnull CloneableObject<?> message, MessagePriority priority) {
            super(clientIndex, topic);
            this.topic = topic;
            this.message = message;
            this.priority = priority;
        }

        CloneableObject<?> getMessage() {
            return message;
        }
        
        MessagePriority getPriority() {
            return priority;
        }
    } 
}
