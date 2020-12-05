package org.sn.myutils.util.concurrent;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.LongStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.sn.myutils.util.concurrent.SerializableLambdaUtils.RunnableInfo;


/**
 * A scheduled executor which runs immediate tasks right away in the internal executor.
 * If the task is scheduled and not serializable, it is submitted to the internal executor.
 * If the task is scheduled and is serializable, it is saved into a time bucket file.
 * The tasks in the current time bucket are scheduled for running right away, and tasks in future time buckets will run later.
 *
 * <p>As a bucket of time approaches, all tasks in that file/bucket are loaded into memory into the current executor.
 * If a task is canceled, it is marked as canceled in the time bucket file, and the loader will not load that task into the current executor.
 *
 * <p>Once a task is done, it is marked as done in the time bucket file.
 * This allows us to not load that task into memory again in case the system crashes and restarts.
 *
 * <p>The current implementation does not handle periodic tasks.
 * So tasks at fixed rate or fixed delay are run in the internal executor and are not stored in time bucket files.
 *
 * <p>This executor converts tasks scheduled with a time unit of NANOSECONDS to MILLISECONDS.
 * So if you create a task A fpr 15ns and a task B for 10ns from now, both will run 0ms from now in the order A then B.
 * The time of loading items from disk is so great that nanosecond precision does not even make sense.
 *
 * <p>This class implements the AutoCloseable interface, which calls shutdownNow and awaitTermination of 1 nanosecond,
 * and closes any open files.
 * It is not necessary to call the close function as files will be closed as program exit anyway,
 * but the unit tests do it each test creates a new TimeBucketScheduledThreadPoolExecutor.
 */
public class TimeBucketScheduledThreadPoolExecutor implements AutoCloseableScheduledExecutorService {
    private interface IndexFileLoader {
        /**
         * Create or overwrite a file.
         *
         * @param filename the file's basename. The real file will be this filename within the folder passed to TimeBucketScheduledThreadPoolExecutor's constructor.
         * @return a buffered writer. It is the caller's responsibility to close this.
         */
        @Nonnull BufferedWriter create(String filename) throws IOException;

        /**
         * Open an existing file.
         *
         * @param filename the file's basename. The real file will be this filename within the folder passed to TimeBucketScheduledThreadPoolExecutor's constructor.
         * @return a buffered reader or null if the file does not exist. It is the caller's responsibility to close this.
         */
        @Nullable BufferedReader open(String filename) throws IOException;
    }

    private interface DataFileLoader {
        /**
         * Open a file for reading and writing.
         *
         * @param filename the file's basename. The real file will be this filename within the folder passed to TimeBucketScheduledThreadPoolExecutor's constructor.
         * @param createIfNotFound If true then if the file does not exist, create an empty file
         * @return a RandomAccessFile or null if createIfNotFound is found and file does not exist. It is the caller's responsibility to close this.
         */
        @Nullable RandomAccessFile open(String filename, boolean createIfNotFound) throws IOException;

        /**
         * Delete a file.
         *
         * @implNote Implementations can assume that the RandomAccessFile is closed.
         *
         * @param filename the file's basename. The real file will be this filename within the folder passed to TimeBucketScheduledThreadPoolExecutor's constructor.
         * @throws IOException if there was an error deleting the file or the file does not exist.
         */
        void delete(String filename) throws IOException;
    }


    /**
     * The class to manage time buckets.
     * This class takes care of:
     * - finding a time bucket for a future task
     * - creating new buckets
     * - loading buckets into memory as time approaches
     * - expiring old buckets
     */
    private static class TimeBucketManager {
        private static final System.Logger LOGGER = System.getLogger(TimeBucketScheduledThreadPoolExecutor.class.getName());
        private static final int TIME_BUCKET_VERSION = 1;
        private static final int MAX_BUCKETS_TO_KEEP_OPEN = 16;

        private enum FutureStatus {
            PENDING((byte) 1),
            CANCELED((byte) 2),
            DONE((byte) 3);

            private final byte val;

            FutureStatus(byte val) {
                this.val = val;
            }

            byte toByte() {
                return val;
            }

            static FutureStatus fromByte(byte val) {
                switch (val) {
                    case 1: return PENDING;
                    case 3: return DONE;
                    default: return CANCELED;
                }
            }
        }

        private final ThreadLocal<TimeBucketFutureTask<?>> threadLocalFutureTask;
        private final ScheduledThreadPoolExecutor mainExecutor;
        private final ScheduledThreadPoolExecutor timeBucketExecutor = new ScheduledThreadPoolExecutor(1, MoreExecutors.createThreadFactory("TimeBucketManager", false));

        private final StampedLock timeBucketsLock = new StampedLock();
        private final List<TimeBucket> timeBuckets = new ArrayList<>();
        private volatile long timeBucketLengthMillis;

        private final IndexFileLoader indexFileLoader;
        private final DataFileLoader dataFileLoader;

        /**
         * A LRU cache of each time bucket to the random access file, which is an open file.
         * When an time bucket is explicitly removed or evicted from the map, we close the file.
         *
         * <p>This class may not be complete, so it is not suitable for general use, and is therefore private.
         *
         * <p>There is no need for finalize/Cleaner because RandomAccessFile registers a cleaner to close the file when it becomes phantom reachable.
         */
        private final Map<TimeBucket, RandomAccessFile> timeBucketFileMap = Collections.synchronizedMap(new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<TimeBucket, RandomAccessFile> entry) {
                if (size() > MAX_BUCKETS_TO_KEEP_OPEN) {
                    closeFile(entry.getKey(), entry.getValue());
                    return true;
                }
                return false;
            }

            @Override
            public RandomAccessFile remove(Object timeBucket) {
                var oldStream = super.remove(timeBucket);
                if (oldStream != null) {
                    closeFile((TimeBucket) timeBucket, oldStream);
                }
                return oldStream;
            }

            @Override
            public void clear() {
                for (var entry : entrySet()) {
                    closeFile(entry.getKey(), entry.getValue());
                }
                super.clear();
            }

            private void closeFile(TimeBucket timeBucket, Closeable stream) {
                try {
                    stream.close();
                } catch (IOException e) {
                    LOGGER.log(ERROR, "Unable to close time bucket file: basename=" + timeBucket.getFilename());
                }
            }
        });

        TimeBucketManager(ThreadLocal<TimeBucketFutureTask<?>> threadLocalFutureTask,
                          ScheduledThreadPoolExecutor mainExecutor,
                          Duration bucketLength,
                          IndexFileLoader indexFileLoader,
                          DataFileLoader dataFileLoader) throws IOException {
            this.threadLocalFutureTask = threadLocalFutureTask;
            this.mainExecutor = mainExecutor;
            this.timeBucketLengthMillis = bucketLength.toMillis();
            this.indexFileLoader = indexFileLoader;
            this.dataFileLoader = dataFileLoader;

            loadTimeBuckets();
        }

        void setTimeBucketLengthMillis(Duration bucketLength) {
            long writeLock = timeBucketsLock.writeLock();
            try {
                this.timeBucketLengthMillis = bucketLength.toMillis();
            } finally {
                timeBucketsLock.unlockWrite(writeLock);
            }
        }

        private class TimeBucketFutureTask<V> implements RunnableScheduledFuture<V> {
            private final TimeBucket timeBucket;
            private final long position;
            private final long whenMillis;
            private @Nullable RunnableScheduledFuture<V> realFuture;
            private boolean canceled;

            TimeBucketFutureTask(TimeBucket timeBucket, long position, long whenMillis) {
                this.timeBucket = timeBucket;
                this.position = position;
                this.whenMillis = whenMillis;
            }

            void setRealFuture(RunnableScheduledFuture<V> realFuture) {
                this.realFuture = realFuture;
            }

            @Override // ScheduledFuture -> Delayed
            public long getDelay(@Nonnull TimeUnit unit) {
                return unit.convert(whenMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            }

            @Override // ScheduledFuture -> Delayed -> Comparable
            @SuppressWarnings("unchecked")
            public int compareTo(@Nonnull Delayed thatObject) {
                if (thatObject instanceof TimeBucketFutureTask) {
                    TimeBucketFutureTask<V> that = (TimeBucketFutureTask<V>) thatObject;
                    return Long.compare(this.whenMillis, that.whenMillis);
                } else {
                    return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), thatObject.getDelay(TimeUnit.MILLISECONDS));
                }
            }

            @Override // Future
            public boolean cancel(boolean mayInterruptIfRunning) {
                if (canceled) {
                    return true;
                }
                boolean localCanceled;
                if (realFuture != null) {
                    localCanceled = realFuture.cancel(mayInterruptIfRunning);
                } else {
                    localCanceled = true;
                }
                try {
                    TimeBucketManager.this.cancel(this);
                    canceled = localCanceled;
                    return localCanceled;
                } catch (IOException e) {
                    throw new CancellationException(e.getMessage());
                }
            }

            @Override // Future
            public boolean isCancelled() {
                return canceled;
            }

            @Override // Future
            public boolean isDone() {
                return canceled || (realFuture != null && realFuture.isDone());
            }

            @Override // Future
            public V get() throws InterruptedException, ExecutionException {
                if (realFuture == null) {
                    realFuture = awaitRealFuture(timeBucket);
                }
                return realFuture.get();
            }

            @Override // Future
            public V get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                long maxWaitNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
                long start = System.nanoTime();
                if (realFuture == null) {
                    realFuture = awaitRealFuture(maxWaitNanos);
                }
                long waitNanos = maxWaitNanos - (System.nanoTime() - start);
                return realFuture.get(waitNanos, TimeUnit.NANOSECONDS);
            }

            /**
             * Run the future task now.
             *
             * @throws NullPointerException if the future is not loaded into memory
             */
            @Override // RunnableScheduledFuture -> RunnableFuture
            public void run() {
                Objects.requireNonNull(realFuture).run();
                try {
                    TimeBucketManager.this.done(timeBucket, position);
                } catch (IOException | RuntimeException | Error e) {
                    LOGGER.log(ERROR, "Unable to mark task as done: timeBucket=" + timeBucket + ", position=" + position + ", error='" + e.toString() + "'");
                }
            }

            @Override // RunnableScheduledFuture
            public boolean isPeriodic() {
                return false;
            }

            @SuppressWarnings("unchecked")
            private @Nonnull RunnableScheduledFuture<V> awaitRealFuture(TimeBucket timeBucket) throws InterruptedException {
                return (RunnableScheduledFuture<V>) timeBucket.pullRealFuture(position);
            }

            @SuppressWarnings("unchecked")
            private @Nonnull RunnableScheduledFuture<V> awaitRealFuture(long nanos) throws InterruptedException, TimeoutException {
                return (RunnableScheduledFuture<V>) timeBucket.pullRealFuture(position, nanos);
            }
        }

        /**
         * Add a new future to a time bucket, creating a time bucket and time bucket data file if necessary.
         *
         * @throws RejectedExecutionException with root cause as IOException if there was an error writing to the data file
         * @throws RejectedExecutionException if there was an error deserializing the data
         */
        TimeBucketFutureTask<?> addToTimeBucket(RunnableInfo info) {
            long now = System.currentTimeMillis();
            long whenMillis = now + info.getTimeInfo().getInitialDelayMillis();
            TimeBucket timeBucket = lookupTimeBucket(whenMillis);
            timeBucket.normalizeInitialDelay(info, whenMillis);
            RandomAccessFile dataStream = lookupTimeBucketDataFile(timeBucket, true);
            timeBucket.dataFileAppendLock.lock();
            try {
                long startPosition = dataStream.length();
                dataStream.seek(startPosition);
                dataStream.writeByte(FutureStatus.PENDING.toByte());
                byte[] runnableInfoAsBytes = info.toBytes();
                dataStream.writeInt(runnableInfoAsBytes.length);
                dataStream.write(runnableInfoAsBytes);
                if (timeBucket.isInMemory()) {
                    return scheduleTaskNow(timeBucket, info, startPosition);
                } else {
                    return new TimeBucketFutureTask<>(timeBucket, startPosition, whenMillis);
                }
            } catch (IOException | SerializableScheduledExecutorService.RecreateRunnableFailedException e) {
                mainExecutor.getRejectedExecutionHandler().rejectedExecution(info.toTaskInfo().getActionAsRunnable(), mainExecutor);
                throw new RejectedExecutionException(e); // TODO: reject
            } finally {
                timeBucket.dataFileAppendLock.unlock();
            }
        }

        /**
         * Schedule loading of a time bucket when now is almost the start time of the bucket.
         * If now is 105ms, time bucket length is 1000ms, and time bucket starts at 3000ms,
         * bucket will be loaded when time is 3000 - (20% of 1000) = 2800ms.
         */
        private void scheduleLoadingOfTimeBucket(TimeBucket timeBucket) {
            long whenMillis = timeBucket.getStartInclusiveMillis() - timeBucketLengthMillis / 5;
            long delayMillis = whenMillis - System.currentTimeMillis();
            LOGGER.log(TRACE, () -> "Schedule loading of time bucket: timeBucket=" + timeBucket + ", whenMillis=" + whenMillis);
            timeBucketExecutor.schedule(() -> {
                if (Thread.currentThread().isInterrupted()) {
                    LOGGER.log(TRACE, "Loading time bucket interrupted: timeBucket=" + timeBucket);
                    return;
                }
                LOGGER.log(TRACE, () -> "Loading time bucket: timeBucket=" + timeBucket);
                Instant startTime = Instant.now();
                RandomAccessFile dataStream = lookupTimeBucketDataFile(timeBucket, false);
                int countSuccess = 0;
                int countFailure = 0;
                try {
                    long fileLength = dataStream.length();
                    timeBucket.setIsInMemory();
                    skipHeaders(dataStream);
                    long position;
                    while ((position = dataStream.getFilePointer()) < fileLength) {
                        if (Thread.currentThread().isInterrupted()) {
                            LOGGER.log(TRACE, "Loading time bucket interrupted within loop: timeBucket=" + timeBucket);
                            break;
                        }
                        FutureStatus futureStatus = FutureStatus.fromByte(dataStream.readByte());
                        byte[] runnableInfoBytes = new byte[dataStream.readInt()];
                        dataStream.readFully(runnableInfoBytes);
                        if (futureStatus == FutureStatus.PENDING) {
                            try {
                                RunnableInfo info = RunnableInfo.fromBytes(runnableInfoBytes);
                                var futureTask = scheduleTaskNow(timeBucket, info, position);
                                countSuccess++;
                                timeBucket.resolveWaitingFuture(position, futureTask);
                            } catch (SerializableScheduledExecutorService.RecreateRunnableFailedException | IOException e) {
                                LOGGER.log(WARNING, "Unable to recreate runnable: timeBucket=" + timeBucket + ", error='" + e.toString() + "'");
                                countFailure++;
                            } finally {
                                threadLocalFutureTask.remove();
                            }
                        }
                    }
                } catch (IOException | RuntimeException | Error e) {
                    LOGGER.log(ERROR, "Error loading time bucket: timeBucket=" + timeBucket, e);
                } finally {
                    Duration timeTaken = Duration.between(startTime, Instant.now());
                    LOGGER.log(
                            TRACE,
                            "Time bucket loaded: timeBucket=" + timeBucket
                                    + ", countSuccess=" + countSuccess
                                    + ", countFailure=" + countFailure
                                    + ", timeTaken=" + timeTaken.toMillis() + "ms");
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
        }

        private TimeBucketFutureTask<Object> scheduleTaskNow(TimeBucket timeBucket,
                                                             RunnableInfo info,
                                                             long startPosition) throws SerializableScheduledExecutorService.RecreateRunnableFailedException {
            long now = System.currentTimeMillis();
            long whenMillis = timeBucket.padInitialDelay(info, now);
            var futureTask = new TimeBucketFutureTask<>(timeBucket, startPosition, whenMillis);
            threadLocalFutureTask.set(futureTask);
            try {
                // call info.apply to call schedule on the main executor
                // this invokes ScheduledThreadPoolExecutor.schedule
                // which creates a ScheduledThreadPoolExecutor.ScheduledFutureTask and calls decorateTask
                // which makes ScheduledThreadPoolExecutor.schedule return this futureTask
                // with the native ScheduledThreadPoolExecutor.ScheduledFutureTask as a member variable of the futureTask
                info.apply(TimeBucketManager.this.mainExecutor); // may throw RecreateRunnableFailedException
                return futureTask;
            } finally {
                threadLocalFutureTask.remove();
            }
        }

        private void cancel(TimeBucketFutureTask<?> futureTask) throws IOException {
            var timeBucket = futureTask.timeBucket;
            RandomAccessFile dataStream = lookupTimeBucketDataFile(timeBucket, false);
            dataStream.seek(futureTask.position);
            dataStream.writeByte(FutureStatus.CANCELED.toByte());
        }

        private void done(TimeBucket timeBucket, long position) throws IOException {
            RandomAccessFile dataStream = lookupTimeBucketDataFile(timeBucket, false);
            dataStream.seek(position);
            dataStream.writeByte(FutureStatus.DONE.toByte());
        }

        /**
         * Return the time bucket that includes the given time.
         * Creates a new bucket if needed.
         *
         * <p>For example suppose bucketLengthMillis=1000ms and we
         * - schedule a future to occur at 3400ms, so a new bucket is created for the range [3000, 4000)
         * - schedule a future to occur at 3900ms, and the bucket above can be used
         * - schedule a future to occur at 5700ms, so a new bucket is created for the range [5000, 6000)
         * - schedule a future to occur at 4800ms, so a new bucket is created for the range [4000, 5000)
         *
         * <p>The algorithm acquires a read lock to find the right time bucket.
         * If a time bucket is not found, we upgrade the read lock to a write lock and insert the new time bucket.
         *
         * @param when when the future occurs
         * @return a time bucket, or null if a new one must be created
         */
        private @Nonnull TimeBucket lookupTimeBucket(long when) {
            int attemptWriteLockIndex = -1;
            long lock = 0;
            try {
                readOrWriteLockLoop: while (true) {
                    try {
                        // acquire read lock or write lock
                        if (attemptWriteLockIndex == -1) {
                            lock = timeBucketsLock.readLock();
                        } else {
                            long writeLock = timeBucketsLock.tryConvertToWriteLock(lock);
                            if (writeLock != 0) {
                                lock = writeLock;
                                break;
                            } else {
                                timeBucketsLock.unlock(lock);
                                lock = timeBucketsLock.writeLock();
                            }
                        }

                        // search for time bucket or the index at which to insert the new time bucket
                        var iter = timeBuckets.listIterator(timeBuckets.size());
                        while (iter.hasPrevious()) {
                            var timeBucket = iter.previous();
                            if (when >= timeBucket.getStartInclusiveMillis()) {
                                if (when < timeBucket.getEndExclusiveMillis()) {
                                    return timeBucket;
                                } else {
                                    attemptWriteLockIndex = iter.nextIndex() +  1; // insert new time bucket after 'timeBucket'
                                    continue readOrWriteLockLoop;
                                }
                            }
                        }

                        attemptWriteLockIndex = 0; // insert new time bucket at start of array
                    } catch (ConcurrentModificationException ignored) {
                        timeBucketsLock.unlock(lock);
                    }
                } // end while
                assert timeBucketsLock.isWriteLocked();
                long bucketStartMillis = when / timeBucketLengthMillis * timeBucketLengthMillis;
                return addTimeBucket(bucketStartMillis, timeBucketLengthMillis, attemptWriteLockIndex, /*forceAsyncLoad*/ false);
            } finally {
                if (timeBucketsLock.isWriteLocked() || timeBucketsLock.isReadLocked()) {
                    timeBucketsLock.unlock(lock);
                }
            }
        }

        private TimeBucket addTimeBucket(long startInclusiveMillis, long durationMillis, int arrayIndex, boolean forceAsyncLoad) {
            var timeBucket = new TimeBucket(startInclusiveMillis, durationMillis);
            LOGGER.log(INFO, () -> "Creating time bucket: timeBucket=" + timeBucket);
            timeBuckets.add(arrayIndex, timeBucket);
            saveTimeBucketsAsync();
            if (forceAsyncLoad || !timeBucket.isInMemory()) {
                scheduleLoadingOfTimeBucket(timeBucket);
            }
            scheduleDeletionOfTimeBucket(timeBucket); // as there is one thread in the time bucket executor, this will always run after load
            return timeBucket;
        }

        /**
         * Write out the file index.txt, which is a list of time buckets.
         * Each time bucket is identified by its start timestamp in milliseconds.
         * For each time bucket there is a data file with the start timestamp plus ".dat" but this function does not work on these files.
         *
         * <p>This function is called when a time bucket is created or deleted.
         * The writing of this file happens asynchronously.
         */
        private void saveTimeBucketsAsync() {
            timeBucketExecutor.submit(() -> {
                long writeLock = timeBucketsLock.writeLock();
                try (PrintWriter writer = new PrintWriter(indexFileLoader.create("index.txt"))) {
                    writer.println("[title]");
                    writer.println("version=" + TIME_BUCKET_VERSION);
                    writer.println();
                    writer.println("[buckets]");
                    timeBuckets.forEach(timeBucket -> writer.println(timeBucket.getStartInclusiveMillis() + " " + timeBucket.getEndExclusiveMillis()));
                    writer.close();
                    if (writer.checkError()) {
                        throw new IOException();
                    }
                } catch (IOException e) {
                    LOGGER.log(ERROR, "Unable to write to index.txt", e);
                } catch (RuntimeException | Error e) {
                    LOGGER.log(ERROR, "Unexpected exception while writing index.txt", e);
                } finally {
                    timeBucketsLock.unlockWrite(writeLock);
                }
            });
        }

        private void loadTimeBuckets() throws IOException {
            long writeLock = timeBucketsLock.writeLock();
            try {
                int lineNumber = 0;
                try (BufferedReader bufferedReader = indexFileLoader.open("index.txt")) {
                    if (bufferedReader != null) {
                        boolean loadingTitle = false;
                        boolean loadingBuckets = false;
                        Integer version = null;
                        while (true) {
                            String line = bufferedReader.readLine();
                            if (line == null) {
                                break;
                            }
                            lineNumber++;
                            if (line.isEmpty()) {
                                loadingTitle = false;
                                loadingBuckets = false;
                            } else if (line.equals("[title]")) {
                                loadingTitle = true;
                            } else if (line.equals("[buckets]")) {
                                Objects.requireNonNull(version);
                                loadingBuckets = true;
                            } else if (loadingTitle) {
                                if (line.startsWith("version=")) {
                                    version = Integer.parseInt(line.substring("version=".length()));
                                }
                            } else if (loadingBuckets) {
                                StringTokenizer tokenizer = new StringTokenizer(line);
                                long startMillis = Long.parseLong(tokenizer.nextToken());
                                long endMillis = Long.parseLong(tokenizer.nextToken());
                                addTimeBucket(startMillis, endMillis - startMillis, timeBuckets.size(), /*forceAsyncLoad*/ true);
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    throw new IOException("Parse error on line " + lineNumber, e);
                }
            } finally {
                timeBucketsLock.unlockWrite(writeLock);
            }
        }

        private @Nonnull RandomAccessFile lookupTimeBucketDataFile(TimeBucket timeBucket, boolean createIfNotFound) {
            return timeBucketFileMap.computeIfAbsent(timeBucket, unused -> {
                try {
                    RandomAccessFile randomAccessFile = Objects.requireNonNull(dataFileLoader.open(timeBucket.getFilename(), createIfNotFound));
                    if (randomAccessFile.length() == 0) {
                        randomAccessFile.writeInt(TIME_BUCKET_VERSION);
                        randomAccessFile.writeLong(timeBucket.getStartInclusiveMillis());
                        randomAccessFile.writeLong(timeBucket.getEndExclusiveMillis());
                    }
                    return randomAccessFile;
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
        }

        /**
         * Stops background tasks of the time bucket executor.
         * These are loading buckets and deleted buckets.
         *
         * @param immediate true means call shutdownNow and close files. false means call shutdown.
         */
        void stopBackgroundTasks(boolean immediate) {
            if (immediate) {
                timeBucketExecutor.shutdownNow();
                timeBuckets.clear();
                timeBucketFileMap.clear(); // closes files
            } else {
                timeBucketExecutor.shutdown();
            }
        }

        /**
         * Wait for background tasks (i.e. loading time bucket file into memory and deleting time bucket file) up to the given timeout to finish.
         * If time bucket A is [1000, 2000), B is [2000, 3000), C is [3000, 4000) and timeout is  2500,
         * then basically call timeBucketExecutor.awaitTermination(2000ms),
         * and return 2500 - 2000 = 500ms.
         *
         * @param timeout the maximum length of time to wait
         * @param unit the length of time unit
         * @return the amount of time in nanoseconds left to wait
         */
        long waitForBackgroundTasksToFinish(long timeout, TimeUnit unit) throws InterruptedException {
            long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
            long startTime = System.nanoTime();
            timeBucketExecutor.awaitTermination(timeout, unit);
            long durationForTimeBucketManagerShutdown = System.nanoTime() - startTime;
            return timeoutNanos - durationForTimeBucketManagerShutdown;
        }

        boolean hasFutureTimeBuckets() {
            long readLock = timeBucketsLock.readLock();
            try {
                return timeBuckets.stream().anyMatch(timeBucket -> !timeBucket.isInMemory());
            } finally {
                timeBucketsLock.unlockRead(readLock);
            }
        }

        private void skipHeaders(RandomAccessFile randomAccessFile) throws IOException {
            randomAccessFile.seek(0);
            randomAccessFile.readInt();
            randomAccessFile.readLong();
            randomAccessFile.readLong();
        }

        /**
         * Schedule deletion of a time bucket when its end time elapses.
         * This entails:
         * - removing the time bucket from the timeBuckets variable.
         * - deleting the ".dat" file
         * - rewriting index.txt to exclude this file
         */
        private void scheduleDeletionOfTimeBucket(TimeBucket timeBucket) {
            long delayMillis = timeBucket.getEndExclusiveMillis() - System.currentTimeMillis();
            LOGGER.log(TRACE, () -> "Schedule deletion of time bucket: timeBucket=" + timeBucket + ", whenMillis=" + timeBucket.getEndExclusiveMillis());
            timeBucketExecutor.schedule(() -> {
                LOGGER.log(TRACE, () -> "Deleting time bucket: timeBucket=" + timeBucket);
                long writeLock = timeBucketsLock.writeLock();
                try {
                    timeBuckets.remove(timeBucket);
                    timeBucketFileMap.remove(timeBucket);
                    dataFileLoader.delete(timeBucket.getFilename());
                    saveTimeBucketsAsync();
                } catch (IOException | RuntimeException | Error e) {
                    LOGGER.log(ERROR, "Error deleting time bucket: timeBucket=" + timeBucket, e);
                } finally {
                    timeBucketsLock.unlockWrite(writeLock);
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
        }

        private LongStream getTimeBuckets() {
            return timeBuckets.stream().mapToLong(TimeBucket::getStartInclusiveMillis);
        }

        private LongStream getTimeBucketOpenFiles() {
            return timeBucketFileMap.keySet().stream().mapToLong(TimeBucket::getStartInclusiveMillis);
        }
    }

    /**
     * Class representing a time bucket.
     * There is no data in this class like an ArrayList of futures as the futures are stored on disk.
     *
     * <p>No two time buckets overlap, so a time bucket is identified by its start time (which is used in equals and hashCode).
     * The end time is used to ensure that time buckets do not overlap, and to schedule the deletion of a time bucket.
     */
    private static class TimeBucket {
        private final long startInclusiveMillis;
        private final long endExclusiveMillis;
        private final Lock dataFileAppendLock = new ReentrantLock();
        private final Map<Long, BlockingValue<TimeBucketManager.TimeBucketFutureTask<?>>> waitingFutures = Collections.synchronizedMap(new HashMap<>());
        private volatile boolean inMemory;

        /**
         * Create a time bucket.
         *
         * @see TimeBucket#isInMemory() for a description of how inMemory is set
         */
        TimeBucket(long startMillis, long durationMillis) {
            this.startInclusiveMillis = startMillis;
            this.endExclusiveMillis = startMillis + durationMillis;
            this.inMemory = startMillis - durationMillis / 5 <= System.currentTimeMillis();
        }

        @Override
        public int hashCode() {
            return (int) startInclusiveMillis;
        }

        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof TimeBucket)) {
                return false;
            }
            TimeBucket that = (TimeBucket) thatObject;
            return this.startInclusiveMillis == that.startInclusiveMillis;
        }

        @Override
        public String toString() {
            return "[" + startInclusiveMillis + "," + endExclusiveMillis + ")[inMemory=" + inMemory + "]";
        }

        long getStartInclusiveMillis() {
            return startInclusiveMillis;
        }

        long getEndExclusiveMillis() {
            return endExclusiveMillis;
        }

        String getFilename() {
            return startInclusiveMillis + ".dat";
        }

        /**
         * Return true if the future should be scheduled to run in the current ScheduledExecutorService.
         * This happens if:
         * - the start time of this bucket is less than or equal to the start time of the future, or
         * - the start time of this bucket is slightly after the time of the future.
         *     if time bucket starts at 4000ms, and bucket length is 1000ms, and now is 3900ms, return true
         *     because 3900 >= 4000 - (20% of 1000) = 3800ms
         */
        boolean isInMemory() {
            return inMemory;
        }

        void setIsInMemory() {
            inMemory = true;
        }


        /**
         * Modify the initial delay of the given RunnableInfo to match the time bucket's start time.
         * For example, if now is 200ms, the initial delay is 1500ms, then the event time is 1700ms (passed in as whenMillis to this function),
         * and if this time bucket starts at 1000ms reduce the initial delay to 1700 - 1000 = 700ms.
         *
         * <p>The unit will be changed to MILLISECONDS.
         *
         * @param whenMillis the time when the event should occur in milliseconds, or 1700ms in our example
         */
        void normalizeInitialDelay(RunnableInfo info, long whenMillis) {
            long initialDelayMillis = whenMillis - startInclusiveMillis;
            info.getTimeInfo().setInitialDelay(initialDelayMillis, TimeUnit.MILLISECONDS);
        }

        /**
         * Modify the initial delay of the given RunnableInfo to account for loading of this time bucket before it is active.
         * For example, if this time bucket starts at 1000ms, the initial delay is 700ms, now is 800ms,
         * then the event starts at 1700ms, so set initial delay to 700 + (1000 - 800) = 900ms
         *
         * <p>This function assumes that the unit is MILLISECONDS.
         *
         * @return the instant when the event will occur in milliseconds
         */
        public long padInitialDelay(RunnableInfo info, long now) {
            assert info.getTimeInfo().getUnit() == TimeUnit.MILLISECONDS;
            long delta;
            delta = startInclusiveMillis - now;
            long initialDelay = info.getTimeInfo().getInitialDelay();
            initialDelay += delta;
            info.getTimeInfo().setInitialDelay(initialDelay);
            return now + initialDelay;
        }

        RunnableScheduledFuture<?> pullRealFuture(long position) throws InterruptedException {
            var blockingValue = waitingFutures.computeIfAbsent(position, unused -> new BlockingValue<>());
            var futureTask = blockingValue.get();
            waitingFutures.remove(position);
            return futureTask.realFuture;
        }

        RunnableScheduledFuture<?> pullRealFuture(long position, long nanos) throws InterruptedException, TimeoutException {
            var blockingValue = waitingFutures.computeIfAbsent(position, unused -> new BlockingValue<>());
            var futureTask = blockingValue.get(nanos, TimeUnit.NANOSECONDS);
            waitingFutures.remove(position);
            return futureTask.realFuture;
        }

        void resolveWaitingFuture(long position, @Nonnull TimeBucketManager.TimeBucketFutureTask<?> future) {
            waitingFutures.computeIfPresent(position, (unusedPosition, blockingValue) -> blockingValue.setValue(future));
        }
    }

    private final ThreadLocal<TimeBucketManager.TimeBucketFutureTask<?>> threadLocalFutureTask = new ThreadLocal<>();
    private final ScheduledThreadPoolExecutor mainExecutor;
    private final TimeBucketManager timeBucketManager;
    private ExecutorState executorState = ExecutorState.ALIVE;

    private enum ExecutorState {
        ALIVE,
        SHUTDOWN,
        TERMINATED
    }

    /**
     * Create a scheduled executor.
     *
     * @param folder the folder housing the index files and data files, where each data file is a list of runnables
     * @param timeBucketLength the initial length of each bucket
     * @param corePoolSize the number of threads in this executor
     * @param threadFactory the thread factory
     * @param rejectedHandler the rejection rejectedHandler
     * @throws IOException if there was an error loading the existing time buckets
     */
    public TimeBucketScheduledThreadPoolExecutor(Path folder,
                                                 Duration timeBucketLength,
                                                 int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler rejectedHandler) throws IOException {
        IndexFileLoader indexFileLoader = new IndexFileLoader() {
            @Override
            public @Nonnull BufferedWriter create(String filename) throws FileNotFoundException {
                File file = new File(folder.toFile(), filename);
                return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
            }

            @Override
            public @Nullable BufferedReader open(String filename) throws IOException {
                File file = new File(folder.toFile(), filename);
                if (!file.exists()) {
                    return null;
                }
                return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            }
        };

        DataFileLoader dataFileLoader = new DataFileLoader() {
            @Override
            public @Nullable RandomAccessFile open(String filename, boolean createIfNotFound) throws FileNotFoundException {
                File file = new File(folder.toFile(), filename);
                if (createIfNotFound || file.exists()) {
                    return new RandomAccessFile(file, "rw");
                } else {
                    return null;
                }
            }

            @Override
            public void delete(String filename) throws IOException {
                File file = new File(folder.toFile(), filename);
                if (!file.delete()) {
                    throw new IOException("Unable to delete data file " + file);
                }
            }
        };

        this.mainExecutor = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory, rejectedHandler) {
            @Override
            @SuppressWarnings("unchecked")
            protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
                task = super.decorateTask(runnable, task);
                TimeBucketManager.TimeBucketFutureTask<V> futureTask = (TimeBucketManager.TimeBucketFutureTask<V>) threadLocalFutureTask.get();
                if (futureTask != null) {
                    futureTask.setRealFuture(task);
                    return futureTask;
                } else {
                    return task;
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
                task = super.decorateTask(callable, task);
                TimeBucketManager.TimeBucketFutureTask<V> futureTask = (TimeBucketManager.TimeBucketFutureTask<V>) threadLocalFutureTask.get();
                if (futureTask != null) {
                    futureTask.setRealFuture(task);
                    task = futureTask;
                }
                return task;
            }

        };

        this.timeBucketManager = new TimeBucketManager(threadLocalFutureTask, mainExecutor, timeBucketLength, indexFileLoader, dataFileLoader);
    }

    public void setTimeBucketLength(Duration timeBucketLength) {
        timeBucketManager.setTimeBucketLengthMillis(timeBucketLength);
    }

    @Override
    public @Nonnull ScheduledFuture<?> schedule(@Nonnull Runnable runnable, long delay, @Nonnull TimeUnit unit) {
        TimeBucketManager.TimeBucketFutureTask<?> futureTask = threadLocalFutureTask.get();
        if (futureTask != null) {
            // we are being called from the call to RunnableInfo.apply in scheduleTaskNow
            return mainExecutor.schedule(runnable, delay, unit); // invokes decorateTask
        } else {
            checkShutdown(runnable);
            if (runnable instanceof Serializable) {
                var info = Objects.requireNonNull(SerializableLambdaUtils.computeRunnableInfo(runnable, delay, 0, unit, WARNING));
                return timeBucketManager.addToTimeBucket(info);
            } else {
                return mainExecutor.schedule(runnable, delay, unit); // invokes decorateTask
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nonnull <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
        // RunnableInfo.apply never calls this function, so threadLocalFutureTask.get() is always null when this code is hit
        checkShutdown(callable);
        if (callable instanceof Serializable) {
            var info = Objects.requireNonNull(SerializableLambdaUtils.computeRunnableInfo(callable, delay, unit, WARNING));
            return (ScheduledFuture<V>) timeBucketManager.addToTimeBucket(info);
        } else {
            return mainExecutor.schedule(callable, delay, unit); // invokes decorateTask
        }
    }

    @Override
    public void shutdown() {
        executorState = ExecutorState.SHUTDOWN;
        timeBucketManager.stopBackgroundTasks(false);
        // we cannot shutdown mainExecutor because during awaitTermination tasks from future time buckets may be added to it
    }

    /**
     * {@inheritdoc}
     *
     * <p>This functions returns the tasks not started of the current time bucket(s).
     * As we approach the end of one time bucket, the other is loaded into memory,
     * so this function would return all the tasks from the second bucket as well as the unfinished ones of the first.
     */
    @Override
    public @Nonnull List<Runnable> shutdownNow() {
        executorState = ExecutorState.SHUTDOWN;
        timeBucketManager.stopBackgroundTasks(true);
        return mainExecutor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executorState.ordinal() >= 1;
    }

    /**
     * {@inheritdoc}
     *
     * <p>This function calls shutdown on the main executor.
     *
     * <p>This function returns false if the tasks in the main executor did not complete or of there is at least one future time bucket not yet loaded into memory.
     * Caveat: if all tasks in the future time bucket(s) have been canceled, this functions still returns false even though there are no more tasks to run.
     */
    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        if (!isShutdown() && !isTerminated()) {
            throw new IllegalStateException("awaitTermination called before shutdown/shutdownNow");
        }
        long durationLeftNanos = timeBucketManager.waitForBackgroundTasksToFinish(timeout, unit);
        mainExecutor.shutdown();
        if (durationLeftNanos <= 100_000) {
            durationLeftNanos = 100_000;
        }
        boolean terminated = mainExecutor.awaitTermination(durationLeftNanos, TimeUnit.NANOSECONDS);
        timeBucketManager.stopBackgroundTasks(true);
        terminated &= !timeBucketManager.hasFutureTimeBuckets();
        if (terminated) {
            executorState = ExecutorState.TERMINATED;
        }
        return terminated;
    }

    @Override
    public boolean isTerminated() {
        return executorState.ordinal() >= 2;
    }

    private void checkShutdown(Runnable runnable) {
        if (isShutdown()) {
            mainExecutor.getRejectedExecutionHandler().rejectedExecution(runnable, mainExecutor);
        }
    }

    private <V> void checkShutdown(Callable<V> callable) {
        checkShutdown(new FutureTask<>(callable));
    }

    // Forwarding functions

    @Override // Executor
    public void execute(@Nonnull Runnable command) {
        checkShutdown(command);
        mainExecutor.execute(command);
    }

    @Override // ExecutorService
    public @Nonnull Future<?> submit(@Nonnull Runnable task) {
        checkShutdown(task);
        return mainExecutor.submit(task);
    }

    @Override // ExecutorService
    public @Nonnull <T> Future<T> submit(@Nonnull Runnable task, T result) {
        checkShutdown(task);
        return mainExecutor.submit(task, result);
    }

    @Override // ExecutorService
    public @Nonnull <T> Future<T> submit(@Nonnull Callable<T> task) {
        checkShutdown(task);
        return mainExecutor.submit(task);
    }

    @Override // ExecutorService
    public @Nonnull <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return mainExecutor.invokeAll(tasks);
    }

    @Override // ExecutorService
    public @Nonnull <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return mainExecutor.invokeAll(tasks, timeout, unit);
    }

    @Override // ExecutorService
    public @Nonnull <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return mainExecutor.invokeAny(tasks);
    }

    @Override // ExecutorService
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout,
                           @Nonnull TimeUnit unit)throws InterruptedException, ExecutionException, TimeoutException {
        return mainExecutor.invokeAny(tasks, timeout, unit);
    }

    @Override // ScheduledExecutorService
    public @Nonnull ScheduledFuture<?> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay, long period, @Nonnull TimeUnit unit) {
        checkShutdown(command);
        return mainExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override // ScheduledExecutorService
    public @Nonnull ScheduledFuture<?> scheduleWithFixedDelay(@Nonnull Runnable command, long initialDelay, long delay, @Nonnull TimeUnit unit) {
        checkShutdown(command);
        return mainExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * Return the number of time buckets.
     * Used for testing.
     */
    LongStream getTimeBuckets() {
        return timeBucketManager.getTimeBuckets();
    }

    /**
     * Return the number of open files.
     * Used for testing.
     */
    LongStream getTimeBucketOpenFiles() {
        return timeBucketManager.getTimeBucketOpenFiles();
    }

    @Override
    public void close() throws IOException {
        shutdownNow();
        try {
            awaitTermination(1, TimeUnit.MICROSECONDS); // closes files
        } catch (InterruptedException | RuntimeException | Error ignored) {
            try {
                awaitTermination(1, TimeUnit.MICROSECONDS); // closes files
            } catch (InterruptedException | RuntimeException | Error e) {
                throw new IOException(e);
            }
        }
    }
}
