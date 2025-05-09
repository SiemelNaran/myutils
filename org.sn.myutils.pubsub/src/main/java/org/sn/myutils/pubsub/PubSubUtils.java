package org.sn.myutils.pubsub;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    private static final System.Logger LOGGER = System.getLogger(PubSubUtils.class.getName());
    private static final Cleaner cleaner = Cleaner.create();
    
    /**
     * A class whose constructor a few lines of the current thread's call stack.
     * Used for recording when an object was created, so that it can be logged upon destruction.
     */
    private static class CallStackCapturing implements Runnable {
        private final Class<? extends Shutdowneable> clazz;
        private final StackTraceElement[] stackTrace;

        CallStackCapturing(Class<? extends Shutdowneable> clazz) {
            this.clazz = clazz;
            StackTraceElement[] fullStackTrace = Thread.currentThread().getStackTrace();
            this.stackTrace = Arrays.stream(fullStackTrace).skip(3).limit(7).toArray(StackTraceElement[]::new);
        }
        
        /**
         * Return the call stack at creation.
         * It starts with a newline and does not end with a newline.
         */
        private String getCallStack() {
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

        @Override
        public void run() {
            LOGGER.log(System.Logger.Level.INFO, "Shutting down {0}", clazz.getSimpleName());
            LOGGER.log(System.Logger.Level.TRACE, "Call stack at creation:" + getCallStack());
        }
    }

    static Cleanable addShutdownHook(Shutdowneable object) {
        CallStackCapturing callStackCapturing = new CallStackCapturing(object.getClass());
        Runnable shutdownAction = object.shutdownAction();
        Runnable runnable = () -> {
            callStackCapturing.run();
            shutdownAction.run();
        };
        return cleaner.register(object, runnable);
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
            if (!(closeable instanceof ObjectOutputStream || closeable instanceof ObjectInputStream)) {
                // don't log "Shutting down java.io.ObjectOutputStream" etc. as they happen so frequently
                LOGGER.log(System.Logger.Level.TRACE, "Shutting down " + closeable);
            }
            closeable.close();
        } catch (IOException | RuntimeException | Error e) {
            LOGGER.log(System.Logger.Level.TRACE, "Error closing " + closeable, e);
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
        double delta = (long) (Math.random() * range + 0.5) - (range / 2);
        return (long) (backoff + delta);
    }
    
    static Long extractClientIndex(MessageBase message) {
        if (message instanceof RelayMessageBase action) {
            return action.getClientIndex();
        }
        return null;
    }

    static @Nullable ServerIndex extractServerIndex(MessageBase message) {
        if (message instanceof RelayMessageBase action) {
            return action.getRelayFields().getServerIndex(); // may throw NullPointerException
        }
        return null;
    }
}
