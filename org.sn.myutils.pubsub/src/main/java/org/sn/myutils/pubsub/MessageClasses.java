package org.sn.myutils.pubsub;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * All of the messages sent between client and server.
 */
public interface MessageClasses {
    
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
     * The base class of all messages that pertain to one topic.
     * 
     * <p>As of 11/1/2020 only client generated messages implement this class, and most implement this class.
     * These classes do not implement TopicMessageBase:
     * - Identification as there is no topic
     * - DownloadPublishedMessages as there are many topics
     */
    interface TopicMessageBase extends MessageBase {
        String getTopic();
    }

    /**
     * If a message can be sent to the server after it was already send and received, it should inherit from this class.
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
            return serverTimestamp; // COVERAGE: missed
        }

        @Override
        public String toLoggingString() {
            return classType(this) + ", serverTimestamp=" + serverTimestamp;
        }
    }
    
    /**
     * Class sent by server when it receives an invalid message.
     * Required field failedClientIndex, which identifies the message which failed, indicating to the client that it should resend this message;
     */
    abstract class AbstractInvalidMessage extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final Class<? extends MessageBase> classOfMessage;
        private final Long failedClientIndex; // optional because not all messages have a clientIndex
        
        AbstractInvalidMessage(Class<? extends MessageBase> classOfMessage, Long failedClientIndex) {
            this.classOfMessage = classOfMessage;
            this.failedClientIndex = failedClientIndex;
        }
        
        Class<? extends MessageBase> getClassOfMessage() {
            return classOfMessage;
        }
        
        Long getFailedClientIndex() {
            return failedClientIndex;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", classOfMessage=" + classOfMessage.getSimpleName() + ", failedClientIndex=" + failedClientIndex;
        }
    }

    /**
     * Class sent by server when it receives an object type it does not know how to handle.
     * This could happen if the client and server are on different versions.
     */
    class UnsupportedMessage extends AbstractInvalidMessage {
        private static final long serialVersionUID = 1L;

        UnsupportedMessage(Class<? extends MessageBase> classOfMessage, Long failedClientIndex) {
            super(classOfMessage, failedClientIndex);
        }
    }

    /**
     * Class sent by server to client to request identification.
     */
    class RequestIdentification extends AbstractInvalidMessage {
        private static final long serialVersionUID = 1L;
        
        RequestIdentification(Class<? extends MessageBase> classOfMessage, Long failedClientIndex) {
            super(classOfMessage, failedClientIndex);
        }
    }

    /**
     * Class sent by server to tell client to start read and write threads.
     */
    class ClientAccepted extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final CentralServerId centralServerId;
        
        ClientAccepted(CentralServerId centralServerId) {
            this.centralServerId = centralServerId;
        }
        
        CentralServerId getCentralServerId() {
            return centralServerId;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", centralServerId=" + centralServerId;
        }
    }

    /**
     * Class sent by server to tell client that it failed to start.
     */
    class ClientRejected extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final CentralServerId centralServerId;
        private final @Nonnull String error;
        
        ClientRejected(CentralServerId centralServerId, @Nonnull String error) {
            this.centralServerId = centralServerId;
            this.error = error;
        }
        
        CentralServerId getCentralServerId() {
            return centralServerId;
        }
        
        @Nonnull String getError() {
            return error;
        }
 
        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", centralServerId=" + centralServerId + ", error='" + error + "'";
        }
        
        PubSubException toException() {
            return new PubSubException(error);
        }
    }

    /**
     * Class sent by server to tell client that a message it sent sent is invalid.
     * Required field error, which is the error message.
     */
    class InvalidMessage extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final @Nonnull String error;

        InvalidMessage(@Nonnull String error) {
            this.error = error;
        }

        public @Nonnull String getError() {
            return error;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", error='" + error + "'";
        }
    }
    
    /**
     * Class to notify the client that the attempt to subscribe or unsubscribe failed,
     * Required fields topic and subscriberName.
     */
    abstract class AddOrRemoveSubscriberFailed extends InvalidMessage {
        private static final long serialVersionUID = 1L;
        
        private final String topic;
        private final String subscriberName;

        public AddOrRemoveSubscriberFailed(String error, String topic, String subscriberName) {
            super(error);
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
     * Class to notify the client that adding a subscriber failed.
     */
    class AddSubscriberFailed extends AddOrRemoveSubscriberFailed {
        private static final long serialVersionUID = 1L;
        
        AddSubscriberFailed(@Nonnull String error, String topic, String subscriberName) {
            super(error, topic, subscriberName);
        }
    }

    /**
     * Class to notify the client that removing a subscriber failed.
     */
    class RemoveSubscriberFailed extends AddOrRemoveSubscriberFailed {
        private static final long serialVersionUID = 1L;
        
        RemoveSubscriberFailed(@Nonnull String error, String topic, String subscriberName) {
            super(error, topic, subscriberName);
        }
    }


    /**
     * Class sent by server to tell client that a relay message it sent was invalid.
     * For example, this could happen if client sends a message to server that server already processed.
     * Required fields failedClientIndex, which identifies the message.
     */
    class InvalidRelayMessage extends InvalidMessage {
        private static final long serialVersionUID = 1L;
        
        private final long failedClientIndex;

        InvalidRelayMessage(@Nonnull String error, long failedClientIndex) {
            super(error);
            this.failedClientIndex = failedClientIndex;
        }

        public long getFailedClientIndex() {
            return failedClientIndex;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", failedClientIndex=" + failedClientIndex;
        }
    }
    
    /**
     * Class to notify the client that creating a publisher failed.
     */
    class CreatePublisherFailed extends InvalidRelayMessage {
        private static final long serialVersionUID = 1L;
        
        private final String topic;

        CreatePublisherFailed(@Nonnull String error, long failedClientIndex, String topic) {
            super(error, failedClientIndex);
            this.topic = topic;
        }

        String getTopic() {
            return topic;
        }
    }

    
    /**
     * Class sent by server to tell client that an action like CreatePublisher or AddSubscriber worked.
     */
    class AbstractConfirmAction extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        AbstractConfirmAction() {
        }
    }

    /**
     * Class sent by server to tell client that CreatePublisher worked.
     */
    class PublisherCreated extends AbstractConfirmAction {
        private static final long serialVersionUID = 1L;
        
        private final String topic;
        private final RelayFields relayFields;
        
        PublisherCreated(String topic, RelayFields relayFields) {
            this.topic = topic;
            this.relayFields = relayFields;
        }

        String getTopic() {
            return topic;
        }
        
        RelayFields getRelayFields() {
            return relayFields;
        }
        
        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", topic=" + topic + ", " + relayFields.toString();
        }
    }

    /**
     * Class sent by server to tell client that subscribe worked.
     */
    class SubscriberAdded extends AbstractConfirmAction {
        private static final long serialVersionUID = 1L;
        
        private final String topic;
        private final String subscriberName;
        
        SubscriberAdded(String topic, String subscriberName) {
            this.topic = topic;
            this.subscriberName = subscriberName;
        }
        
        String getTopic() {
            return topic;
        }
        
        String getSubscriberName() {
            return subscriberName;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", topic=" + topic + ", subscriberName=" + subscriberName;
        }
    }


    /**
     * Class sent by server to tell client that unsubscribe worked.
     */
    class SubscriberRemoved extends AbstractConfirmAction {
        private static final long serialVersionUID = 1L;
        
        private final String topic;
        private final String subscriberName;
        
        SubscriberRemoved(String topic, String subscriberName) {
            this.topic = topic;
            this.subscriberName = subscriberName;
        }

        String getTopic() {
            return topic;
        }
        
        String getSubscriberName() {
            return subscriberName;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", topic=" + topic + ", subscriberName=" + subscriberName;
        }
    }

    //////////////////////////////////////////////////////////////////////
    // Client generated messages
    
    /**
     * Messages originating from client and sent to server, which they may be relayed to other clients.
     * Required field clientTimestamp, which is the time the client sent the message.
     */
    abstract class ClientGeneratedMessage implements MessageBase {
        private static final long serialVersionUID = 1L;
        
        private final long clientTimestamp;
        private Map<String, String> customProperties;

        ClientGeneratedMessage(Long clientTimestamp) {
            this.clientTimestamp = clientTimestamp != null ? clientTimestamp : System.currentTimeMillis();
        }

        long getClientTimestamp() {
            return clientTimestamp;
        }
        
        void setCustomProperties(Map<String, String> customProperties) {
            this.customProperties = customProperties;
        }
        
        Map<String, String> getCustomProperties() {
            return Collections.unmodifiableMap(customProperties);
        }

        @Override
        public String toLoggingString() {
            return classType(this) + ", clientTimestamp=" + clientTimestamp + ", customProperties.size=" + (customProperties != null ? customProperties.size() : 0);
        }
    }

    /**
     * Class sent by client to register itself to the server.
     * Sent when client first connects to server.
     * Required field machineId, which must be unique across all machines.
     */
    class Identification extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final ClientMachineId machineId;
        
        Identification(ClientMachineId machineId) {
            super(null);
            this.machineId = machineId;
        }
        
        ClientMachineId getMachineId() {
            return machineId;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", machineId=" + machineId;
        }
    }
    
    /**
     * Class sent by class if it has a topic.
     * This is most messages.
     */
    abstract class ClientGeneratedTopicMessage extends ClientGeneratedMessage implements TopicMessageBase {
        private static final long serialVersionUID = 1L;
        
        private final String topic;

        public ClientGeneratedTopicMessage(Long clientTimestamp, String topic) {
            super(clientTimestamp);
            this.topic = topic;
        }
        
        @Override
        public String getTopic() {
            return topic;
        }
        
        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", topic=" + topic;
        }
    }

    /**
     * Class to notify the server that we are subscribing to or unsubscribing from a particular topic,
     * so that it sends or stops sending us messages published to this topic.
     * Required fields topic and subscriberName.
     */
    abstract class AddOrRemoveSubscriber extends ClientGeneratedTopicMessage {
        private static final long serialVersionUID = 1L;
        
        private final String subscriberName;

        public AddOrRemoveSubscriber(Long clientTimestamp, String topic, String subscriberName) {
            super(clientTimestamp, topic);
            this.subscriberName = subscriberName;
        }
        
        String getSubscriberName() {
            return subscriberName;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", subscriberName=" + subscriberName;
        }
    }
    
    /**
     * Class to notify the server that we are subscribing to a particular topic.
     * Required field shouldTryDownload.
     * Required field isResend (true if server died and client is resending its addSubscriber commands).
     */
    class AddSubscriber extends AddOrRemoveSubscriber implements Resendable {
        private static final long serialVersionUID = 1L;
        
        private final boolean tryDownload;
        private final boolean isResend;
        
        public AddSubscriber(long createdAtTimestamp, String topic, String subscriberName, boolean tryDownload, boolean isResend) {
            super(createdAtTimestamp, topic, subscriberName);
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
            return super.toLoggingString() + ", tryDownload=" + tryDownload + ", isResend=" + isResend;
        }
    }
    
    /**
     * Class to notify the server that we are unsubscribing from a particular topic.
     * No new required fields.
     */
    class RemoveSubscriber extends AddOrRemoveSubscriber {
        private static final long serialVersionUID = 1L;
        
        public RemoveSubscriber(String topic, String subscriberName) {
            super(null, topic, subscriberName);
        }
    }
    
    /**
     * Class to fetch a publisher from the server.
     * Required field topic.
     * The response is a CreatePublisher command.
     */
    class FetchPublisher extends ClientGeneratedTopicMessage {
        private static final long serialVersionUID = 1L;
        
        public FetchPublisher(String topic) {
            super(null, topic);
        }
    }
    
    /**
     * Class sent by client to download published messages even if they have already been sent to the client.
     * The server will send a PublishMessage for each object that it has in its cache.
     */
    class DownloadPublishedMessages extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final Collection<String> topics;
        private final ServerIndex startServerIndexInclusive;
        private final ServerIndex endServerIndexInclusive;

        public DownloadPublishedMessages(Collection<String> topics, ServerIndex startServerIndexInclusive, ServerIndex endServerIndexInclusive) {
            super(null);
            this.topics = topics;
            this.startServerIndexInclusive = startServerIndexInclusive;
            this.endServerIndexInclusive = endServerIndexInclusive;
        }
        
        Collection<String> getTopics() {
            return topics;
        }
        
        ServerIndex getStartServerIndexInclusive() {
            return startServerIndexInclusive;
        }
        
        ServerIndex getEndServerIndexInclusive() {
            return endServerIndexInclusive;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", startServerIndexInclusive=" + startServerIndexInclusive + ", endServerIndexInclusive=" + endServerIndexInclusive;
        }
        
        /**
         * Split a message to download topics A, B, C according to the topics on each message server.
         * If message server one hosts topics A and C, and message server two hosts topics B and D.
         * call this function to create two download commands.
         */
        DownloadPublishedMessages cloneTo(Collection<String> topicsSublist) {
            assert topics.containsAll(topicsSublist);
            return new DownloadPublishedMessages(topicsSublist, startServerIndexInclusive, endServerIndexInclusive);
        }
    }

    /**
     * Fields set by the server when it receives a relay message (CreatePublisher or PublishMessage).
     * Required field serverTimestamp, which is the time the server received the message.
     * Required field serverIndex, which is the unique monotonically increasing integer identifying the message.
     * May not correlate with serverTimestamp.
     * Required field sourceMachineId, which is the client machine that sent the message.
     */
    class RelayFields implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final long serverTimestamp;
        private final ServerIndex serverIndex;
        private final ClientMachineId sourceMachineId;
        
        RelayFields(long serverTimestamp, ServerIndex serverIndex, ClientMachineId sourceMachineId) {
            this.serverTimestamp = serverTimestamp;
            this.serverIndex = serverIndex;
            this.sourceMachineId = sourceMachineId;
        }
        
        long getServerTimestamp() {
            return serverTimestamp; // COVERAGE: missed
        }
        
        ServerIndex getServerIndex() {
            return serverIndex;
        }

        ClientMachineId getSourceMachineId() {
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
     * Required field clientIndex, which is the message number on the client.
     * Optional field relayFields, which is null when the client has not yet sent the message to the server, and not null once the server processes it.
     * It is unique across messages in all machines.
     */
    abstract class RelayMessageBase extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;

        private final long clientIndex;
        private RelayFields relayFields; // null before message sent to server
        
        RelayMessageBase(Long createdAtTimestamp, long clientIndex) {
            super(createdAtTimestamp);
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

        @Override
        public String toLoggingString() {
            return super.toLoggingString()
                    + ", clientIndex=" + clientIndex
                    + ", " + relayFields;
        }
    }
    
    /**
     * Relay messages with a topic.
     */
    abstract class RelayTopicMessageBase extends RelayMessageBase implements TopicMessageBase {
        private static final long serialVersionUID = 1L;

        private final @Nonnull String topic;
        
        RelayTopicMessageBase(Long createdAtTimestamp, long clientIndex, @Nonnull String topic) {
            super(null, clientIndex);
            this.topic = topic;
        }
        
        @Override
        public @Nonnull String getTopic() {
            return topic;
        }
        
        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", topic=" + topic;
        }
    }
    
    /**
     * Action representing the createPublisher command.
     * Sent to a client when they subscribe to a topic,
     * or when they issue the FetchPublisher command.
     */
    class CreatePublisher extends RelayTopicMessageBase implements Resendable {
        private static final long serialVersionUID = 1L;
        
        private final @Nonnull Class<?> publisherClass;
        private final boolean isResend;

        CreatePublisher(long createdAtTimestamp, long clientIndex, @Nonnull String topic, @Nonnull Class<?> publisherClass, boolean isResend) {
            super(createdAtTimestamp, clientIndex, topic);
            this.publisherClass = publisherClass;
            this.isResend = isResend;
        }

        @Nonnull Class<?> getPublisherClass() {
            return publisherClass;
        }

        @Override
        public boolean isResend() {
            return isResend;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", publisherClass=" + publisherClass.getSimpleName() + ", isResend=" + isResend;
        }
    }
    
    
    /**
     * Action representing the publisher.publish command.
     */
    class PublishMessage extends RelayTopicMessageBase {
        private static final long serialVersionUID = 1L;
        
        private final @Nonnull CloneableObject<?> message;
        private final @Nonnull RetentionPriority priority;
        
        PublishMessage(long clientIndex, @Nonnull String topic, @Nonnull CloneableObject<?> message, @Nonnull RetentionPriority priority) {
            super(null, clientIndex, topic);
            this.message = message;
            this.priority = priority;
        }

        @Nonnull CloneableObject<?> getMessage() {
            return message;
        }
        
        RetentionPriority getRetentionPriority() {
            return priority;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", message.estimateBytes=" + message.getNumBytes() + ", retentionPriority=" + priority;
        }
    } 
}
