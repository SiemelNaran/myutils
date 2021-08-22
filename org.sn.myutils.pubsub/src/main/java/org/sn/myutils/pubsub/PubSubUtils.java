package org.sn.myutils.pubsub;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.Nullable;
import org.sn.myutils.pubsub.MessageClasses.MessageBase;
import org.sn.myutils.pubsub.MessageClasses.RelayMessageBase;


class PubSubUtils {
    private static final Cleaner cleaner = Cleaner.create();
    
    /**
     * A class whose constructor a few lines of the current thread's call stack.
     * Used for recording when an object was created, so that it can be logged upon destruction.
     */
    abstract static class CallStackCapturing {
        private final StackTraceElement[] stackTrace;

        CallStackCapturing() {
            StackTraceElement[] fullStackTrace = Thread.currentThread().getStackTrace();
            this.stackTrace = Arrays.stream(fullStackTrace).skip(3).limit(7).toArray(StackTraceElement[]::new);
        }
        
        /**
         * Return the call stack at creation.
         * It starts with a newline and does not end with a newline.
         */
        String getCallStack() {
            StringBuilder builder = new StringBuilder("\n");
            for (var element : stackTrace) {
                builder.append("\tat ")
                       .append(element.getClassName()).append('.').append(element.getMethodName())
                       .append('(').append(element.getFileName()).append(':').append(element.getLineNumber())
                       .append(")\n");
            }
            builder.deleteCharAt(builder.length() - 1);
            return builder.toString();
        }
    }
    
    static Cleanable addShutdownHook(Object object, Runnable cleanup, Class<?> clazz) {
        Cleanable cleanable = cleaner.register(object, cleanup);
        Thread thread = new Thread(cleanable::clean, clazz.getSimpleName() + ".shutdown");
        Runtime.getRuntime().addShutdownHook(thread);
        return cleanable;
    }
    
    static String getLocalAddress(NetworkChannel channel) {
        if (channel == null) {
            return "null";
        }
        try {
            return Objects.toString(channel.getLocalAddress());
        } catch (IOException e) {
            return "<unknown>";
        }
    }

    static String getRemoteAddress(SocketChannel channel) {
        if (channel == null) {
            return "null";
        }
        try {
            return Objects.toString(channel.getRemoteAddress());
        } catch (IOException e) {
            return "<unknown>";
        }
    }

    static String getRemoteAddress(AsynchronousSocketChannel channel) {
        if (channel == null) {
            return "null";
        }
        try {
            return Objects.toString(channel.getRemoteAddress());
        } catch (IOException e) {
            return "<unknown>";
        }
    }

    static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException | RuntimeException | Error ignored) {
        }
    }
    
    static void closeExecutorQuietly(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        try {
            executor.shutdownNow();
            //noinspection ResultOfMethodCallIgnored
            executor.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | RuntimeException | Error ignored) {
        }
    }

    public static void unlockSafely(@NotNull Lock lock) throws IllegalMonitorStateException {
        try {
            lock.unlock();
        } catch (IllegalMonitorStateException ignored) {
        }
    }

    /**
     * Calculate the exponential backoff delay.
     * 
     * @param base the time in any unit for the first retry
     * @param retryNumber one for the first call to this function
     * @param capRetries if retryNumber is more than capRetries then set retryNumber to capRetires, to prevent exponential backoff from becoming too big
     * @return the exponential backoff in milliseconds
     */
    static long computeExponentialBackoff(long base, int retryNumber, int capRetries) {
        int count = Math.min(retryNumber,  capRetries) - 1;
        return base * (1L << count);
    }
    
    /**
     * Calculate the exponential backoff delay.
     *
     * @param base the time in any unit for the first retry
     * @param retryNumber one for the first call to this function
     * @param capRetries if retryNumber is more than capRetries then set retryNumber to capRetires, to prevent exponential backoff from becoming too big
     * @param jitterFraction add a small positive/negative random value to the returned value. Should typically be between [0, 0.20].
     * @return the exponential backoff in milliseconds
     */
    static long computeExponentialBackoff(long base, int retryNumber, int capRetries, double jitterFraction) {
        long backoff = computeExponentialBackoff(base, retryNumber, capRetries);
        double range = backoff * jitterFraction;
        long delta = (long) (Math.random() * range + 0.5);
        delta -= range / 2;
        return backoff + delta;
    }
    
    static Long extractClientIndex(MessageBase message) {
        if (message instanceof RelayMessageBase) {
            RelayMessageBase action = (RelayMessageBase) message;
            return action.getClientIndex();
        }
        return null;
    }

    static @Nullable ServerIndex extractServerIndex(MessageBase message) {
        if (message instanceof RelayMessageBase) {
            RelayMessageBase action = (RelayMessageBase) message;
            return action.getRelayFields().getServerIndex(); // may throw NullPointerException
        }
        return null;
    }
}
