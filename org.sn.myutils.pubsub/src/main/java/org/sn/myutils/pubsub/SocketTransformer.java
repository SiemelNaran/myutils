package org.sn.myutils.pubsub;

import static org.sn.myutils.pubsub.PubSubUtils.closeQuietly;
import static org.sn.myutils.util.ExceptionUtils.unwrapCompletionException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.sn.myutils.pubsub.MessageClasses.MessageBase;
import org.sn.myutils.pubsub.MessageClasses.MessageWrapper;


class SocketTransformer {
    private static final Void NULL = null;
    
    private static class MessageAsByteBuffers {
        private final ByteBuffer lengthBuffer = ByteBuffer.allocate(2);
        private final ByteBuffer messageBuffer;

        <T> MessageAsByteBuffers(T message, short maxLength) throws IOException {
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
    public void writeMessageToSocket(MessageBase message, short maxLength, SocketChannel channel) throws IOException {
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
    public MessageWrapper readMessageFromSocket(SocketChannel channel) throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Short.BYTES);
        readAllBytes(channel, lengthBuffer);
        short length = lengthBuffer.getShort();
        
        ByteBuffer messageBuffer = ByteBuffer.allocate(length);
        readAllBytes(channel, messageBuffer);
        return byteBufferToMessage(messageBuffer, MessageWrapper.class);
    }
    
    
    private static void readAllBytes(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                throw new EOFException("end of stream: read " + buffer.position() + " bytes, expected " + buffer.capacity());
            }
        }
        buffer.flip();
    }

    static class WriteSocketException extends Exception {
        private WriteSocketException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Write a message to a socket asynchronously.
     * The first 2 bytes is the length of the message.
     * The next bytes are the message.
     *
     * @param message the message to write, which must implement Serializable
     * @param maxLength the maximum length of the message
     * @param channel the channel to write to
     * @return a completion stage resolved with null if the write was successful, or rejected with the exception if the write failed
     * @throws IllegalArgumentException if the message is too long
     * @throws IOException if there was an IOException writing to the object output stream or to the socket
     */
    public CompletionStage<Void> writeMessageToSocketAsync(MessageWrapper message, short maxLength, AsynchronousSocketChannel channel) throws IOException {
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
                        futureMessage.completeExceptionally(new WriteSocketException(e));
                    }
                    
                });
            }

            @Override
            public void failed(Throwable e, Void unused) {
                futureMessage.completeExceptionally(new WriteSocketException(e));
            }
        });
        return futureMessage;
    }

    static class ReadSocketException extends Exception {
        private ReadSocketException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Read a message from a socket asynchronously.
     * The first 2 bytes is the length of the message.
     * The next bytes are the message.
     * 
     * @param channel the channel to read from
     * @return a completion stage resolved with the MessageBase if the read was successful, or rejected with the exception if the read failed
     */
    public CompletionStage<MessageBase> readMessageFromSocketAsync(AsynchronousSocketChannel channel) {
        CompletableFuture<MessageBase> futureMessage = new CompletableFuture<>();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Short.BYTES);
        channel.read(lengthBuffer, NULL, new CompletionHandler<>() {
            @Override
            public void completed(Integer lengthBufferLength, Void unused) {
                if (lengthBufferLength < Short.BYTES) {
                    futureMessage.completeExceptionally(new ReadSocketException(new EOFException("end of stream: read " + lengthBuffer.capacity() + " bytes, expected " + lengthBufferLength)));
                }
                try {
                    lengthBuffer.flip();
                    short length = lengthBuffer.getShort();
                    ByteBuffer messageBuffer = ByteBuffer.allocate(length);
                    
                    channel.read(messageBuffer, NULL, new CompletionHandler<>() {
                        @Override
                        public void completed(Integer messageBufferLength, Void unused) {
                            if (messageBufferLength < length) {
                                futureMessage.completeExceptionally(new ReadSocketException(new EOFException("end of stream: read " + messageBuffer.capacity() + " bytes, got " + messageBufferLength)));
                            }
                            try {
                                messageBuffer.flip();
                                MessageBase message = byteBufferToMessage(messageBuffer, MessageBase.class);
                                futureMessage.complete(message);
                            } catch (IOException | RuntimeException | Error e) {
                                futureMessage.completeExceptionally(new ReadSocketException(e));
                            }
                        }
    
                        @Override
                        public void failed(Throwable e, Void unused) {
                            futureMessage.completeExceptionally(new ReadSocketException(e));
                        }
                        
                    });
                    
                } catch (RuntimeException | Error e) {
                    futureMessage.completeExceptionally(new ReadSocketException(e));
                }
            }

            @Override
            public void failed(Throwable e, Void unused) {
                futureMessage.completeExceptionally(new ReadSocketException(e));
            }
        });
        return futureMessage;
    }
    
    private static <T> ByteBuffer messageToByteBuffer(T message, int maxLength) throws IOException {
        ObjectOutputStream oos = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(message);
            byte[] array = bos.toByteArray();
            if (array.length > maxLength) {
                throw new IllegalArgumentException("message is too long: " + array.length + " > " + maxLength); // COVERAGE: missed
            }
            return ByteBuffer.wrap(bos.toByteArray());
        } finally {
            closeQuietly(oos);
        }
    }

    /**
     * Convert a byte buffer into T using Java serialization.
     * 
     * @param <T> is either MessageBase.class or MessageBaseWrapper.class
     * @throws ClassCastException if buffer translates into an object other than T or one derived from it.
     */
    @SuppressWarnings("unchecked")
    private static <T> T byteBufferToMessage(ByteBuffer buffer, Class<T> clazz) throws IOException {
        ObjectInputStream ois = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(buffer.array());
            ois = new ObjectInputStream(bis);
            Object obj = ois.readObject();
            if (clazz.isInstance(obj)) {
                return (T) obj;
            } else {
                throw new ClassCastException(); 
            }
        } catch (ClassNotFoundException | InvalidClassException | StreamCorruptedException | OptionalDataException | ClassCastException e) {
            throw new IOException(e);
        } finally {
            closeQuietly(ois);
        }
    }
    
    /**
     * Tell if the exception reflects the fact that the socket is closed.
     * The list includes EOFException and all the channel exceptions that have the word Closed in them.
     */
    static boolean isClosed(Throwable throwable) {
        Throwable e = unwrapCompletionException(throwable);
        return e instanceof EOFException || e instanceof ClosedChannelException;
    }
}
