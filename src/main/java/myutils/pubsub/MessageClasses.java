package myutils.pubsub;

import java.io.Serializable;
import javax.annotation.Nonnull;


/**
 * All of the messages sent between client and server.
 */
interface MessageClasses {
    
    /**
     * A method toLoggingString that can be used for logging purposes.
     * Derived classes should only log meta-data, not the content of messages as this may include personally identifiable information.
     */
    interface LoggingString {
        String toLoggingString();
    }
    
    /**
     * This base class of all messages sent between client and server.
     */
    interface MessageBase extends Serializable, LoggingString {
    }
    
    /**
     * If a message can be resent to the server after it was already send and received, it should inherit from this class.
     */
    interface Resendable {
        boolean isResend();
    }
    
    private static String classType(MessageBase message) {
        return "class=" + message.getClass().getSimpleName();
    }
    
    
    //////////////////////////////////////////////////////////////////////
    // Server generated messages

    /**
     * Messages originating from server and sent to client.
     * Required field serverTimestamp, which is the time the server generated the message.
     */
    abstract class ServerGeneratedMessage implements MessageBase {
        private static final long serialVersionUID = 1L;

        private final long serverTimestamp;

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

        @Override
        public String toLoggingString() {
            return classType(this) + ", classOfMessageToResend=" + classOfMessageToResend.getSimpleName() + ", failedClientIndex=" + failedClientIndex;
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

        @Override
        public String toLoggingString() {
            return classType(this) + ", failedClientIndex=" + failedClientIndex + ", error='" + error + "'";
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
        
        private final long clientTimestamp;

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

        @Override
        public String toLoggingString() {
            return classType(this) + ", machineId=" + machineId;
        }
    }

    /**
     * Class to notify the server that we are subscribing to or unsubscribing from a particular topic,
     * so that it sends or stops sending us messages published to this topic.
     * Required fields topic and subscriberName.
     */
    abstract class AddOrRemoveSubscriber extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final String topic;
        private final String subscriberName;

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

        private String basicLoggingString() {
            return classType(this) + ", topic=" + topic + ", subscriberName=" + subscriberName;
        }
    }
    
    /**
     * Class to notify the server that we are subscribing to a particular topic.
     * Required field shouldTryDownload.
     */
    class AddSubscriber extends AddOrRemoveSubscriber implements Resendable {
        private static final long serialVersionUID = 1L;
        
        private final boolean tryDownload;
        private final boolean isResend;
        
        public AddSubscriber(String topic, String subscriberName, boolean tryDownload, boolean isResend) {
            super(topic, subscriberName);
            this.tryDownload = tryDownload;
            this.isResend = isResend;
        }

        public boolean shouldTryDownload() {
            return tryDownload;
        }

        @Override
        public boolean isResend() {
            return isResend;
        }
        
        @Override
        public String toLoggingString() {
            return super.basicLoggingString() + ", tryDownload=" + tryDownload + ", isResend=" + isResend;
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

        @Override
        public String toLoggingString() {
            return super.basicLoggingString();
        }
    }
    
    /**
     * Class to fetch a publisher from the server.
     * Required field topic.
     * The response is a CreatePublisher command.
     */
    class FetchPublisher extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final String topic;
        
        public FetchPublisher(String topic) {
            this.topic = topic;
        }
        
        String getTopic() {
            return topic;
        }

        @Override
        public String toLoggingString() {
            return classType(this) + ", topic=" + topic;
        }
    }
    
    /**
     * Class sent by client to download published messages even if they have already been sent to the client.
     * The server will send a PublishMessage for each object that it has in its cache.
     */
    class DownloadPublishedMessages extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final long startServerIndexInclusive;
        private final long endServerIndexInclusive;

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

        @Override
        public String toLoggingString() {
            return classType(this) + ", startServerIndexInclusive=" + startServerIndexInclusive + ", endServerIndexInclusive=" + endServerIndexInclusive;
        }
    }
    
    class RelayFields implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final long serverTimestamp;
        private final long serverIndex;
        private final String sourceMachineId;
        
        RelayFields(long serverTimestamp, long serverIndex, String sourceMachineId) {
            this.serverTimestamp = serverTimestamp;
            this.serverIndex = serverIndex;
            this.sourceMachineId = sourceMachineId;
        }
        
        long getServerTimestamp() {
            return serverTimestamp;
        }
        
        long getServerIndex() {
            return serverIndex;
        }

        String getSourceMachineId() {
            return sourceMachineId;
        }

        @Override
        public String toString() {
            return "serverTimestamp=" + serverTimestamp
                    + ", serverIndex=" + serverIndex
                    + ", sourceMachineId=" + sourceMachineId;
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

        private final long clientIndex;
        private RelayFields relayFields; // null before message sent to server
        
        RelayMessageBase(long clientIndex) {
            this.clientIndex = clientIndex;
        }
        
        void setRelayFields(RelayFields relayFields) {
            this.relayFields = relayFields;
        }
        
        long getClientIndex() {
            return clientIndex;
        }
        
        RelayFields getRelayFields() {
            return relayFields;
        }

        String basicLoggingString() {
            return classType(this)
                    + ", clientIndex=" + clientIndex
                    + ", " + relayFields;
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
        
        @Override
        String basicLoggingString() {
            return super.basicLoggingString() + ", topic=" + topic;
        }
    }
    
    /**
     * Action representing the createPublisher command.
     * Sent to a client when they subscribe to a topic,
     * or when they issue the FetchPublisher commnand.
     */
    class CreatePublisher extends RelayTopicMessageBase implements Resendable {
        private static final long serialVersionUID = 1L;
        
        private final @Nonnull Class<?> publisherClass;
        private final boolean isResend;

        CreatePublisher(long clientIndex, @Nonnull String topic, @Nonnull Class<?> publisherClass, boolean isResend) {
            super(clientIndex, topic);
            this.publisherClass = publisherClass;
            this.isResend = isResend;
        }

        Class<?> getPublisherClass() {
            return publisherClass;
        }

        @Override
        public boolean isResend() {
            return isResend;
        }

        @Override
        public String toLoggingString() {
            return super.basicLoggingString() + ", publisherClass=" + publisherClass.getSimpleName() + ", isResend=" + isResend;
        }
    }
    
    
    /**
     * Action representing the publisher.publish command.
     */
    class PublishMessage extends RelayTopicMessageBase {
        private static final long serialVersionUID = 1L;
        
        private final @Nonnull CloneableObject<?> message;
        private final @Nonnull RetentionPriority priority;
        
        PublishMessage(long clientIndex, @Nonnull String topic, @Nonnull CloneableObject<?> message, RetentionPriority priority) {
            super(clientIndex, topic);
            this.message = message;
            this.priority = priority;
        }

        CloneableObject<?> getMessage() {
            return message;
        }
        
        RetentionPriority getPriority() {
            return priority;
        }

        @Override
        public String toLoggingString() {
            return super.basicLoggingString() + ", message.estimateBytes=" + message.getNumBytes() + ", retentionPriority=" + priority;
        }
    } 
}
