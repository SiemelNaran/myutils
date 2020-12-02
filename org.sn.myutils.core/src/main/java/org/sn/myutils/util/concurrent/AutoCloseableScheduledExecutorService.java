package org.sn.myutils.util.concurrent;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;


public interface AutoCloseableScheduledExecutorService extends ScheduledExecutorService, AutoCloseable {
    @Override
    void close() throws IOException;
}
