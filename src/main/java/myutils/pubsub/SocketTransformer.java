package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.closeQuietly;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import myutils.pubsub.MessageClasses.MessageBase;


public class SocketTransformer {
    private static Void NULL = null;
    
    private static class MessageAsByteBuffers {
        private final ByteBuffer lengthBuffer = ByteBuffer.allocate(2);
        private final ByteBuffer messageBuffer;

        MessageAsByteBuffers(MessageBase message, short maxLength) throws IOException {
            assert message instanceof Serializable;
            messageBuffer = messageToByteBuffer(message, maxLength);
            lengthBuffer.putShort((short) messageBuffer.limit());
            lengthBuffer.flip();
        }
    }
    
    /**
     * Write a message to a socket synchronously.
     * The first 2 bytes is the length of the message.
     * The next bytes are the message.
     * 
     * @param message the message to write, which must implement Serializable
     * @param maxLength the maximum length of the message
     * @param channel the channel to write to
     * @throws IllegalArgumentException if the message is too long
     * @throws IOException if there was an IOException writing to the object output stream or to the socket
     */
    static void writeMessageToSocket(MessageBase message, short maxLength, SocketChannel channel) throws IOException {
        var byteBuffers = new MessageAsByteBuffers(message, maxLength); 
        writeAllBytes(channel, byteBuffers.lengthBuffer);
        writeAllBytes(channel, byteBuffers.messageBuffer);
    }
        
    private static void writeAllBytes(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
    
    /**
     * Read a message from a socket synchronously.
     * The first 2 bytes is the length of the message.
     * The next bytes are the message.
     * 
     * @param channel the channel to read from
     * @return a MessageBase
     * @throws IOException if there was an IOException or the class not found or does not inherit from MessageBase
     */
    static MessageBase readMessageFromSocket(SocketChannel channel) throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Short.BYTES);
        readAllBytes(channel, lengthBuffer);
        short length = lengthBuffer.getShort();
        
        ByteBuffer messageBuffer = ByteBuffer.allocate(length);
        readAllBytes(channel, messageBuffer);
        return byteBufferToMessage(messageBuffer);
    }
    
    
    private static void readAllBytes(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                throw new EOFException("enf of stream: read " + buffer.position() + " bytes, expecting " + buffer.capacity());
            }
        }
        buffer.flip();
    }

    /**
     * Write a message to a socket asynchronously.
     * The first 2 bytes is the length of the message.
     * The next bytes are the message.
     * 
     * @param message the message to write, which must implement Serializable
     * @param maxLength the maximum length of the message
     * @param channel the channel to write to
     * @throws IllegalArgumentException if the message is too long
     * @throws IOException if there was an IOException writing to the object output stream or to the socket
     */
    public static CompletionStage<Void> writeMessageToSocketAsync(MessageBase message, short maxLength, AsynchronousSocketChannel channel) throws IOException {
        CompletableFuture<Void> futureMessage = new CompletableFuture<>();
        var byteBuffers = new MessageAsByteBuffers(message, maxLength); 
        channel.write(byteBuffers.lengthBuffer, NULL, new CompletionHandler<>() {
            @Override
            public void completed(Integer lengthBufferLength, Void unused) {
                channel.write(byteBuffers.messageBuffer, NULL, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer messageBufferLength, Void unused) {
                        futureMessage.complete(NULL);
                    }

                    @Override
                    public void failed(Throwable e, Void unused) {
                        futureMessage.completeExceptionally(e);
                    }
                    
                });
            }

            @Override
            public void failed(Throwable e, Void unused) {
                futureMessage.completeExceptionally(e);
            }
        });
        return futureMessage;
    }
    
    /**
     * Read a message from a socket asynchronously.
     * The first 2 bytes is the length of the message.
     * The next bytes are the message.
     * 
     * @param channel the channel to read from
     * @return a completion stage resolved with the MessageBase
     */
    public static CompletionStage<MessageBase> readMessageFromSocketAsync(AsynchronousSocketChannel channel) {
        CompletableFuture<MessageBase> futureMessage = new CompletableFuture<>();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Short.BYTES);
        channel.read(lengthBuffer, NULL, new CompletionHandler<>() {
            @Override
            public void completed(Integer lengthBufferLength, Void unused) {
                assert lengthBufferLength == Short.BYTES;
                lengthBuffer.flip();
                short length = lengthBuffer.getShort();
                ByteBuffer messageBuffer = ByteBuffer.allocate(length);
                channel.read(messageBuffer, NULL, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer messageBufferLength, Void unused) {
                        messageBuffer.flip();
                        try {
                            MessageBase message = byteBufferToMessage(messageBuffer);
                            futureMessage.complete(message);
                        } catch (IOException | RuntimeException e) {
                            futureMessage.completeExceptionally(e);
                        }
                    }

                    @Override
                    public void failed(Throwable e, Void unused) {
                        futureMessage.completeExceptionally(e);
                    }
                    
                });
            }

            @Override
            public void failed(Throwable e, Void unused) {
                futureMessage.completeExceptionally(e);
            }
        });
        return futureMessage;
    }
    
    private static ByteBuffer messageToByteBuffer(MessageBase message, int maxLength) throws IOException {
        ObjectOutputStream oos = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(message);
            byte[] array = bos.toByteArray();
            if (array.length > maxLength) {
                throw new IllegalArgumentException("message is too long: " + array.length + " > " + maxLength);
            }
            return ByteBuffer.wrap(bos.toByteArray());
        } finally {
            closeQuietly(oos);
        }
    }

    private static MessageBase byteBufferToMessage(ByteBuffer buffer) throws IOException {
        ObjectInputStream ois = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(buffer.array());
            ois = new ObjectInputStream(bis);
            return (MessageBase) ois.readObject();
        } catch (ClassNotFoundException | InvalidClassException | StreamCorruptedException | OptionalDataException | ClassCastException e) {
            throw new IOException(e);
        } finally {
            closeQuietly(ois);
        }
    }
}
