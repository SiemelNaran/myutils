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
    
    
    /**
     * Messages originating from client and sent to server, which they may be relayed to other clients.
     * Required field clientTimestamp, which is the time the client sent the message.
     * Required field serverTimestamp, which is the time the server received the message, or 0 if the message is yet to be sent to the server.
     */
    abstract class ClientGeneratedMessage implements MessageBase {
        private static final long serialVersionUID = 1L;
        
        private long clientTimestamp;
        private long serverTimestamp;

        ClientGeneratedMessage() {
            this.clientTimestamp = System.currentTimeMillis();
        }

        void setServerTimestampToNow() {
            this.serverTimestamp = System.currentTimeMillis();
        }
        
        long getClientTimestamp() {
            return clientTimestamp;
        }
        
        long getServerTimestamp() {
            return serverTimestamp;
        }
    }

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

    //////////////////////////////////////////////////////////////////////
    // Server generated messages

    /**
     * Class sent by server to client to request identification.
     * Sent when server receives a command from a machine it does not know about.
     * Required field failedClientIndex, which identifies the message which failed, indicating to the client that it should resend this message;
     */
    class RequestIdentification extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final Class<? extends MessageBase> classOfMessageToResend;
        private final Long failedClientIndex;
        
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
    
    //////////////////////////////////////////////////////////////////////
    // Client generated messages
    
    /**
     * Class to notify the server that we are subscribing to a particular topic, so that it sends us messages published to this topic.
     * Required field topic and subscriberName.
     */
    class AddOrRemoveSubscriber extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private boolean add;
        private String topic;
        private String subscriberName;

        public AddOrRemoveSubscriber(boolean add, String topic, String subscriberName) {
            this.add = add;
            this.topic = topic;
            this.subscriberName = subscriberName;
        }
        
        boolean isAddSubscriber() {
            return add;
        }
        
        boolean isRemoveSubscriber() {
            return !add;
        }
        
        String getTopic() {
            return topic;
        }
        
        String getSubscriberName() {
            return subscriberName;
        }
    }
    
    /**
     * Class send by client to download published messages with server index startIndex.
     * The server will send a PublishMessage object for each object that it has in its cache.
     */
    class DownloadPublishedMessages extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private long startIndex;

        public DownloadPublishedMessages(long startIndex) {
            this.startIndex = startIndex;
        }
        
        long getStartIndex() {
            return startIndex;
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

        private String sourceMachineId;
        private long index;
        
        RelayMessageBase() {
        }
        
        RelayMessageBase(long index) {
            this.index = index;
        }
        
        void setSourceMachineIdAndResetIndex(String sourceMachineId, long index) {
            this.sourceMachineId = sourceMachineId;
            this.index = index;
        }
        
        String getSourceMachineId() {
            return sourceMachineId;
        }

        long getIndex() {
            return index;
        }
    }
    
    /**
     * Relay messages with a topic.
     */
    abstract class RelayTopicMessageBase extends RelayMessageBase {
        private static final long serialVersionUID = 1L;

        private final @Nonnull String topic;
        
        RelayTopicMessageBase(long index, @Nonnull String topic) {
            super(index);
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
