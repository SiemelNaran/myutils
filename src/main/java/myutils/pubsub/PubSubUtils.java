package myutils.pubsub;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


class PubSubUtils {
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

    static boolean isClosed(Throwable e) {
        return e instanceof ClosedChannelException || e instanceof AsynchronousCloseException || e instanceof ClosedByInterruptException;
    }
}
