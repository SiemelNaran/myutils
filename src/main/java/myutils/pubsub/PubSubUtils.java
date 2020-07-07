package myutils.pubsub;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


class PubSubUtils {
    private static final Cleaner cleaner = Cleaner.create();
    
    static abstract class CallStackCapturing {
        private final StackTraceElement[] stackTrace;

        CallStackCapturing() {
            StackTraceElement[] fullStackTrace = Thread.currentThread().getStackTrace();
            this.stackTrace = Arrays.stream(fullStackTrace).skip(3).limit(7).toArray(StackTraceElement[]::new);
        }
        
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
        Thread thread = new Thread(() -> cleanable.clean(), clazz.getSimpleName() + ".shutdown");
        Runtime.getRuntime().addShutdownHook(thread);
        return cleanable;
    }
    
    static String getLocalAddress(NetworkChannel channel) {
        try {
            return channel.getLocalAddress().toString();
        } catch (IOException e) {
            return "<unknown>";
        }
    }

    static String getRemoteAddress(SocketChannel channel) {
        try {
            return channel.getRemoteAddress().toString();
        } catch (IOException e) {
            return "<unknown>";
        }
    }

    static String getRemoteAddress(AsynchronousSocketChannel channel) {
        try {
            return channel.getRemoteAddress().toString();
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
            executor.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | RuntimeException | Error ignored) {
        }
    }

    static boolean isClosed(Throwable throwable) {
        Throwable e = throwable instanceof CompletionException ? throwable.getCause() : throwable;
        return e instanceof EOFException || e instanceof ClosedChannelException || e instanceof AsynchronousCloseException || e instanceof ClosedByInterruptException;
    }
}
