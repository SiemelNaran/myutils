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
    
    /**
     * Class sent by server to client to request identification.
     * Sent when server receives a command from a machine it does not know about.
     * Required field failedClientIndex, which identifies the message which failed, indicating to the client that it should resend this message;
     */
    class RequestIdentification extends ServerGeneratedMessage {
        private static final long serialVersionUID = 1L;
        
        private final long failedClientIndex;
        
        RequestIdentification(long failedClientIndex) {
            this.failedClientIndex = failedClientIndex;
        }
        
        long getFailedClientIndex() {
            return failedClientIndex;
        }
    }
    
    
    /**
     * Base class of all actions sent from one client to server, and then forwarded to all other clients.
     * Required field sourceMachineId, which is the machine sending the message. The field is set by server.
     * Required field clientIndex, which is the message number on the client.
     * Required field serverIndex, which is the message number on the server, or 0 if the message is yet to be sent to the server.
     * It is unique across messages in all machines. 
     */
    abstract class ActionMessageBase extends ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;

        private String sourceMachineId;
        private long index;
        
        ActionMessageBase() {
        }
        
        ActionMessageBase(long index) {
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
     * Action representing the createPublisher command.
     */
    class CreatePublisher extends ActionMessageBase {
        private static final long serialVersionUID = 1L;
        
        final @Nonnull String topic;
        final @Nonnull Class<?> publisherClass;

        CreatePublisher(long clientIndex, @Nonnull String topic, @Nonnull Class<?> publisherClass) {
            super(clientIndex);
            this.topic = topic;
            this.publisherClass = publisherClass;
        }

        String getTopic() {
            return topic;
        }

        Class<?> getPublisherClass() {
            return publisherClass;
        }
    }
    
    
    /**
     * Action representing the publisher.publish command.
     */
    class PublishMessage extends ActionMessageBase {
        private static final long serialVersionUID = 1L;
        
        final @Nonnull String topic;
        final @Nonnull CloneableObject<?> message;
        
        PublishMessage(long clientIndex, @Nonnull String topic, @Nonnull CloneableObject<?> message) {
            super(clientIndex);
            this.topic = topic;
            this.message = message;
        }

        String getTopic() {
            return topic;
        }

        CloneableObject<?> getMessage() {
            return message;
        }
    } 
}
