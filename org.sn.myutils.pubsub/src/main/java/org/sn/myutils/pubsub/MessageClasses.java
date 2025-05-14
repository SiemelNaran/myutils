package org.sn.myutils.pubsub;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import org.sn.myutils.annotations.NotNull;


/**
 * All the messages sent between client and server.
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
     * If a message can be sent to the server after it was already sent and received, it should inherit from this class.
     */
    interface Resendable {
        boolean isResend();
    }
    
    abstract class MessageBaseLoggingString implements MessageBase {
        @Serial
        private static final long serialVersionUID = 1L;

        MessageBaseLoggingString() {
        }

        @Override
        public String toLoggingString() {
            return classType(this);
        }
    }
    
    private static <T> String classType(T message) {
        return "class=" + message.getClass().getSimpleName();
    }
    
    // Server generated messages

    /**
     * Messages originating from server and sent to client.
     * Required field serverTimestamp, which is the time the server generated the message.
     */
    abstract class ServerGeneratedMessage extends MessageBaseLoggingString {
        @Serial
        private static final long serialVersionUID = 1L;

        private final long serverTimestamp;

        ServerGeneratedMessage() {
            this.serverTimestamp = System.currentTimeMillis();
        }

        long getServerTimestamp() {
            return serverTimestamp;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", serverTimestamp=" + serverTimestamp;
        }
    }
    
    /**
     * Class sent by server when it receives an invalid message.
     * Required field failedClientIndex, which identifies the message which failed, indicating to the client that it should resend this message;
     */
    abstract class AbstractInvalidMessage extends ServerGeneratedMessage {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final Class<?> classOfMessage;
        private final Long failedClientIndex; // optional because not all messages have a clientIndex
        
        AbstractInvalidMessage(Class<?> classOfMessage, Long failedClientIndex) {
            this.classOfMessage = classOfMessage;
            this.failedClientIndex = failedClientIndex;
        }
        
        Class<?> getClassOfMessage() {
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
     * This could happen if we added a new message class that the client can send but forgot to write the server code to handle this message.
     */
    class UnhandledMessage extends AbstractInvalidMessage {
        @Serial
        private static final long serialVersionUID = 1L;

        UnhandledMessage(Class<? extends MessageBase> classOfMessage, Long failedClientIndex) {
            super(classOfMessage, failedClientIndex);
        }
    }

    class InternalServerError extends ServerGeneratedMessage {
        @Serial
        private static final long serialVersionUID = 1L;

        private final @NotNull String exceptionString;

        private InternalServerError(Throwable throwable, boolean includeCallStack) {
            super();
            this.exceptionString = includeCallStack ? generateShortStackTrace(throwable) : throwable.toString();
        }

        public @NotNull String getExceptionString() {
            return exceptionString;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", exceptionString=\n" + exceptionString;
        }

        /**
         * Generate a string similar to throwable.printStackTrace() but show only the first
         * 5 lines the stack trace and its causes.
         */
        private static String generateShortStackTrace(Throwable throwable) {
            StringBuilder result = new StringBuilder();
            int limit = 1;
            Set<Throwable> throwables = new HashSet<>();
            while (throwable != null && !throwables.contains(throwable)) {
                if (!throwables.isEmpty()) {
                    result.append("Caused by: ");
                }
                result.append(throwable).append('\n');
                if (!(throwable instanceof CompletionException)) {
                    Arrays.stream(throwable.getStackTrace())
                          .limit(limit)
                          .forEach(stackTraceElement -> result.append("\t")
                                                              .append(stackTraceElement.toString())
                                                              .append('\n'));
                }
                throwables.add(throwable);
                throwable = throwable.getCause();
                limit = 5;
            }
            return result.toString();
        }

        static class InternalServerErrorException extends Exception {
            @Serial
            private static final long serialVersionUID = 1L;

            private final UUID errorId;

            public InternalServerErrorException(String topLevelMessage, Throwable e) {
                super(topLevelMessage, e);
                errorId = UUID.randomUUID();
            }

            public String getMessage() {
                return "An internal server error occurred " + super.getMessage() + " with errorId=" + errorId;
            }

            public InternalServerError toMessage(boolean includeCallStack) {
                return new InternalServerError(this, includeCallStack);
            }
        }
    }

    /**
     * Class sent by server to client to request identification.
     * This happens for example if the client creates or publisher or subscribes before sending their identification.
     * Clients send their identification right after connecting, but the server may receive messages out of order,
     * so the server may receive a CreatePublisher message before the Identification message.
     */
    class RequestIdentification extends AbstractInvalidMessage {
        @Serial
        private static final long serialVersionUID = 1L;
        
        RequestIdentification(Class<? extends MessageBase> classOfMessage, Long failedClientIndex) {
            super(classOfMessage, failedClientIndex);
        }
    }

    /**
     * Class sent by server to tell client to start read and write threads.
     */
    class ClientAccepted extends ServerGeneratedMessage {
        @Serial
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
     * Class sent by server to tell client that it failed to register.
     * This happens if a second client sends an Identification with the same name as a previous registered client.
     */
    class ClientRejected extends ServerGeneratedMessage {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final CentralServerId centralServerId;
        private final @NotNull String error;

        ClientRejected(CentralServerId centralServerId, @NotNull String error) {
            this.centralServerId = centralServerId;
            this.error = error;
        }
        
        CentralServerId getCentralServerId() {
            return centralServerId;
        }
        
        @NotNull String getError() {
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
     * Class sent by server to tell client that a message it sent is invalid.
     * Required field error, which is the error message.
     */
    abstract class AbstractCommandFailed extends ServerGeneratedMessage {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final @NotNull String error;

        AbstractCommandFailed(@NotNull String error) {
            this.error = error;
        }

        public @NotNull String getError() {
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
    abstract class AddOrRemoveSubscriberFailed extends AbstractCommandFailed {
        @Serial
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
        @Serial
        private static final long serialVersionUID = 1L;
        
        AddSubscriberFailed(@NotNull String error, String topic, String subscriberName) {
            super(error, topic, subscriberName);
        }
    }

    /**
     * Class to notify the client that removing a subscriber failed.
     */
    class RemoveSubscriberFailed extends AddOrRemoveSubscriberFailed {
        @Serial
        private static final long serialVersionUID = 1L;
        
        RemoveSubscriberFailed(@NotNull String error, String topic, String subscriberName) {
            super(error, topic, subscriberName);
        }
    }

    class DownloadFailed extends AbstractCommandFailed {
        @Serial
        private static final long serialVersionUID = 1L;

        private final int commandIndex;
        private final @NotNull String downloadRequest;

        DownloadFailed(int commandIndex, @NotNull String downloadRequest, @NotNull String error) {
            super(error);
            this.commandIndex = commandIndex;
            this.downloadRequest = downloadRequest;
        }

        public int getCommandIndex() {
            return commandIndex;
        }

        public @NotNull String getDownloadRequest() {
            return downloadRequest;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", commandIndex=" + commandIndex + ", downloadRequest={" + downloadRequest + "}";
        }
    }

    /**
     * Class sent by server to tell client that a relay message it sent was invalid.
     * For example, this could happen if client sends a message to server that server already processed.
     * Required fields failedClientIndex, which identifies the message.
     */
    class InvalidRelayMessage extends AbstractCommandFailed {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final long failedClientIndex;

        InvalidRelayMessage(@NotNull String error, long failedClientIndex) {
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
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final String topic;

        CreatePublisherFailed(@NotNull String error, long failedClientIndex, String topic) {
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
        @Serial
        private static final long serialVersionUID = 1L;
        
        AbstractConfirmAction() {
        }
    }

    /**
     * Class sent by server to tell client that CreatePublisher worked.
     */
    class PublisherCreated extends AbstractConfirmAction {
        @Serial
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
        @Serial
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
        @Serial
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

    
    // Client generated messages

    /**
     * The base class of all client generated messages that perform a one time command.
     */
    interface ClientCommand {
        /**
         * A unique number identifying the command.
         */
        int getCommandIndex();
    }

    /**
     * Messages originating from client and sent to server, which they may be relayed to other clients.
     * Required field clientTimestamp, which is the time the client sent the message.
     */
    abstract class ClientGeneratedMessage extends MessageBaseLoggingString {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final long clientTimestamp;
        @SuppressWarnings("serial")
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
            return super.toLoggingString() + ", clientTimestamp=" + clientTimestamp + ", customProperties.size=" + (customProperties != null ? customProperties.size() : 0);
        }
    }

    /**
     * Class sent by client to register itself to the server.
     * Sent when client first connects to server.
     * Required field machineId, which must be unique across all machines.
     */
    class Identification extends ClientGeneratedMessage {
        @Serial
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
     * Class sent by client to get server to throw an exception.
     * Used for testing.
     */
    class MakeServerThrowAnException extends ClientGeneratedMessage {
        @Serial
        private static final long serialVersionUID = 1L;

        private final String runtimeExceptionMessage;

        MakeServerThrowAnException(String runtimeExceptionMessage) {
            super(null);
            this.runtimeExceptionMessage = runtimeExceptionMessage;
        }

        public String getRuntimeExceptionMessage() {
            return runtimeExceptionMessage;
        }
    }
    
    /**
     * Class sent by class if it has a topic.
     * This is most messages.
     */
    abstract class ClientGeneratedTopicMessage extends ClientGeneratedMessage implements TopicMessageBase {
        @Serial
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

    abstract class ClientGeneratedTopicMessageCommand extends ClientGeneratedTopicMessage implements ClientCommand {
        @Serial
        private static final long serialVersionUID = 1L;

        private final int commandIndex;

        public ClientGeneratedTopicMessageCommand(Long clientTimestamp, String topic, int commandIndex) {
            super(clientTimestamp, topic);
            this.commandIndex = commandIndex;
        }

        @Override
        public int getCommandIndex() {
            return commandIndex;
        }
    }

    /**
     * Class to notify the server that we are subscribing to or unsubscribing from a particular topic,
     * so that it sends or stops sending us messages published to this topic.
     * Required fields topic and subscriberName.
     */
    abstract class AddOrRemoveSubscriber extends ClientGeneratedTopicMessageCommand {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final String subscriberName;

        public AddOrRemoveSubscriber(Long clientTimestamp, String topic, String subscriberName, int commandIndex) {
            super(clientTimestamp, topic, commandIndex);
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
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final boolean tryDownload;
        private final boolean isResend;

        public AddSubscriber(long createdAtTimestamp, String topic, String subscriberName, int commandIndex, boolean tryDownload, boolean isResend) {
            super(createdAtTimestamp, topic, subscriberName, commandIndex);
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
        @Serial
        private static final long serialVersionUID = 1L;
        
        public RemoveSubscriber(String topic, String subscriberName, int commandIndex) {
            super(null, topic, subscriberName, commandIndex);
        }
    }
    
    /**
     * Class to fetch a publisher from the server.
     * Required field topic.
     * The response is a CreatePublisher command.
     */
    class FetchPublisher extends ClientGeneratedTopicMessageCommand {
        @Serial
        private static final long serialVersionUID = 1L;
        
        public FetchPublisher(String topic, int commandIndex) {
            super(null, topic, commandIndex);
        }
    }

    /**
     * Class sent by client to download published messages even if they have already been sent to the client.
     * The server will send a PublishMessage for each object that it has in its cache.
     */
    abstract class DownloadPublishedMessages<DownloadType extends DownloadPublishedMessages<DownloadType>> extends ClientGeneratedMessage implements ClientCommand {
        @Serial
        private static final long serialVersionUID = 1L;

        private final int commandIndex;
        @SuppressWarnings("serial")
        private final Collection<String> topics;

        public DownloadPublishedMessages(int commandIndex, Collection<String> topics) {
            super(null);
            this.commandIndex = commandIndex;
            this.topics = topics;
        }

        Collection<String> getTopics() {
            return topics;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", topics=" + topics;
        }

        @Override
        public int getCommandIndex() {
            return commandIndex;
        }

        abstract DownloadType cloneTo(Collection<String> topicsSublist);
    }

    /**
     * {@inheritdoc}
     *
     * <p>Here the client is downloading messages within an id range.
     */
    class DownloadPublishedMessagesByServerId extends DownloadPublishedMessages<DownloadPublishedMessagesByServerId> {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final @NotNull ServerIndex startServerIndexInclusive;
        private final @NotNull ServerIndex endServerIndexInclusive;

        public DownloadPublishedMessagesByServerId(int commandIndex,
                                                   Collection<String> topics,
                                                   @NotNull ServerIndex startServerIndexInclusive,
                                                   @NotNull ServerIndex endServerIndexInclusive) {
            super(commandIndex, topics);
            this.startServerIndexInclusive = startServerIndexInclusive;
            this.endServerIndexInclusive = endServerIndexInclusive;
        }
        
        @NotNull ServerIndex getStartServerIndexInclusive() {
            return startServerIndexInclusive;
        }
        
        @NotNull ServerIndex getEndServerIndexInclusive() {
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
        @Override
        DownloadPublishedMessagesByServerId cloneTo(Collection<String> topicsSublist) {
            assert getTopics().containsAll(topicsSublist);
            return new DownloadPublishedMessagesByServerId(getCommandIndex(), topicsSublist, startServerIndexInclusive, endServerIndexInclusive);
        }
    }

    /**
     * {@inheritdoc}
     *
     * <p>Here the client is downloading messages within an id range.
     */
    class DownloadPublishedMessagesByClientTimestamp extends DownloadPublishedMessages<DownloadPublishedMessagesByClientTimestamp> {
        @Serial
        private static final long serialVersionUID = 1L;

        private final long startInclusive;
        private final long endInclusive;

        public DownloadPublishedMessagesByClientTimestamp(int commandIndex,
                                                          Collection<String> topics,
                                                          long startInclusive,
                                                          long endInclusive) {
            super(commandIndex, topics);
            this.startInclusive = startInclusive;
            this.endInclusive = endInclusive;
        }

        long getStartInclusive() {
            return startInclusive;
        }

        long getEndInclusive() {
            return endInclusive;
        }

        @Override
        public String toLoggingString() {
            return super.toLoggingString() + ", startInclusive=" + startInclusive + ", endInclusive=" + endInclusive;
        }

        /**
         * Split a message to download topics A, B, C according to the topics on each message server.
         * If message server one hosts topics A and C, and message server two hosts topics B and D.
         * call this function to create two download commands.
         */
        @Override
        DownloadPublishedMessagesByClientTimestamp cloneTo(Collection<String> topicsSublist) {
            assert getTopics().containsAll(topicsSublist);
            return new DownloadPublishedMessagesByClientTimestamp(getCommandIndex(), topicsSublist, startInclusive, endInclusive);
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
        @Serial
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
        @Serial
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
        @Serial
        private static final long serialVersionUID = 1L;

        private final @NotNull String topic;
        
        RelayTopicMessageBase(Long createdAtTimestamp, long clientIndex, @NotNull String topic) {
            super(null, clientIndex);
            this.topic = topic;
        }
        
        @Override
        public @NotNull String getTopic() {
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
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final @NotNull Class<?> publisherClass;
        private final boolean isResend;

        CreatePublisher(long createdAtTimestamp, long clientIndex, @NotNull String topic, @NotNull Class<?> publisherClass, boolean isResend) {
            super(createdAtTimestamp, clientIndex, topic);
            this.publisherClass = publisherClass;
            this.isResend = isResend;
        }

        @NotNull Class<?> getPublisherClass() {
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
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final @NotNull CloneableObject<?> message;
        private final @NotNull RetentionPriority priority;
        
        PublishMessage(long clientIndex, @NotNull String topic, @NotNull CloneableObject<?> message, @NotNull RetentionPriority priority) {
            super(null, clientIndex, topic);
            this.message = message;
            this.priority = priority;
        }

        @NotNull CloneableObject<?> getMessage() {
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

    // Wrapper for server generated messages
    
    /**
     * Class wrapping a MessageBase with additional information that is not intended to be saved.
     * All messages sent from server to client are of type MessageBaseWrapper.
     * All messages sent from client to server are of type ClientGeneratedMessage.
     */
    class MessageWrapper implements Serializable, LoggingString {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final @NotNull MessageBase message;

        MessageWrapper(@NotNull MessageBase message) {
            this.message = message;
        }
        
        public @NotNull MessageBase getMessage() {
            return message;
        }
        
        @Override
        public String toLoggingString() {
            if (message instanceof InternalServerError) {
                return classType(message);
            }
            return classType(message) + " [" + message.toLoggingString() + "]";
        }

        public String getClassTypeOnly() {
            return classType(this);
        }
    }

    /**
     * Class wrapping a RelayMessageBase with additional information, used when sending a message from server to client.
     * The field download is the true if this message is being sent as a result of download,
     * or false otherwise.
     */
    class RelayMessageWrapper extends MessageWrapper {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private final Integer downloadCommandIndex;

        RelayMessageWrapper(@NotNull RelayMessageBase relayMessage, Integer downloadCommandIndex) {
            super(relayMessage);
            this.downloadCommandIndex = downloadCommandIndex;
        }
        
        @Override
        public @NotNull RelayMessageBase getMessage() {
            return (@NotNull RelayMessageBase) super.getMessage();
        }
        
        public Integer getDownloadCommandIndex() {
            return downloadCommandIndex;
        }
        
        @Override
        public String toLoggingString() {
            return super.toLoggingString() + " [downloadCommandIndex=" + downloadCommandIndex + "]";
        }
    }
}
