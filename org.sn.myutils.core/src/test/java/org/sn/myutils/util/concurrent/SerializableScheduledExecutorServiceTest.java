package org.sn.myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.TestUtil;
import org.sn.myutils.util.concurrent.SerializableScheduledExecutorService.RecreateRunnableFailedException;
import org.sn.myutils.util.concurrent.SerializableScheduledExecutorService.UnfinishedTasks;


public class SerializableScheduledExecutorServiceTest {

    private static List<AtomicInteger> createNumbers(int count) {
        List<AtomicInteger> numbers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            numbers.add(new AtomicInteger());
        }
        return numbers;
    }



    // Only a static class or top level class can be created by class.newInstance().
    // You should explicitly declare a constructor, public or package private to avoid a NoSuchMethodException.
    // SerializableScheduledThreadPoolExecutor will only serialize runnables with a public default constructor.
    //
    // Runnables created via a lambdas appear to have only a private constructor with no arguments in Java 8.
    //
    private static class TestBasicRunnable implements Runnable {
        private static AtomicInteger number = new AtomicInteger(); 

        public TestBasicRunnable() {
        }

        @Override
        public void run() {
            number.incrementAndGet();                
        }                    
    }

    @Test
    void testBasicRunnable()
            throws InstantiationException, IllegalAccessException, IllegalArgumentException,
                   InvocationTargetException, NoSuchMethodException, SecurityException {
        Runnable runnableZero = new TestBasicRunnable();
        runnableZero.run();
        assertEquals(1, TestBasicRunnable.number.get());
        
        Class<? extends Runnable> clazz = runnableZero.getClass();
        Runnable instance = clazz.getDeclaredConstructor().newInstance();
        instance.run();
        assertEquals(2, TestBasicRunnable.number.get());
    }


    
    @SuppressWarnings("checkstyle:RightCurlyAlone")
    private static class TestRunnablesWithDefaultConstructor {
        private static final List<AtomicInteger> numbers = createNumbers(4);
        
        private static class FirstRunnable implements Runnable {
            public FirstRunnable() { }
            
            @Override
            public void run() {
                numbers.get(0).incrementAndGet();                
            }                    
        }

        private static class SecondRunnable implements Runnable {
            public SecondRunnable() { }
            
            @Override
            public void run() {
                numbers.get(1).incrementAndGet();                
            }                    
        }

        private static class ThirdRunnable implements Runnable {
            public ThirdRunnable() { }
            
            @Override
            public void run() {
                numbers.get(2).incrementAndGet();                
            }                    
        }

        private static class FourthRunnable implements Runnable {
            public FourthRunnable() { }
            
            @Override
            public void run() {
                numbers.get(3).incrementAndGet();                
            }                    
        }
    }

    @Test
    void testNormalRunnable() throws InterruptedException, IOException, ClassNotFoundException, RecreateRunnableFailedException {
        Runnable runnableOne = new TestRunnablesWithDefaultConstructor.FirstRunnable();
        Runnable runnableTwo = new TestRunnablesWithDefaultConstructor.SecondRunnable();
        Runnable runnableThree = new TestRunnablesWithDefaultConstructor.ThirdRunnable();
        Runnable runnableFour = new TestRunnablesWithDefaultConstructor.FourthRunnable();

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        {
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.schedule(runnableOne, 600, TimeUnit.MILLISECONDS);
            service.schedule(runnableOne, 1610, TimeUnit.MILLISECONDS);
            final ScheduledFuture<?> future1620 = service.schedule(runnableOne, 1620, TimeUnit.MILLISECONDS); // cancelled below
            service.schedule(runnableOne, 2630, TimeUnit.MILLISECONDS);
            service.scheduleAtFixedRate(runnableTwo, 50, 800, TimeUnit.MILLISECONDS);
            service.scheduleWithFixedDelay(runnableThree, 60, 800, TimeUnit.MILLISECONDS);
            ScheduledFuture<?> recurringFuture70 = service.scheduleAtFixedRate(runnableFour, 70, 800, TimeUnit.MILLISECONDS); // cancelled below
            
            Thread.sleep(1000);
            // runnable1: 600 runs, then is done
            // runnable1: 1610 outside the time frame so does not run, pending at end of time frame
            // runnable1: 1620 outside the time frame so does not run; future is cancelled below before calling shutdownNow
            // runnable1: 2630 outside the time frame so does not run, pending at end of time frame
            // runnable2: 50 runs, 850 runs, 1650 outside time frame so does not run, pending at end of time frame
            // runnable3: 60 runs, 860+little runs, 1660+little outside time frame so does not run, pending at end of time frame
            // runnable4: 70 runs, 870 runs, 1670 outside time frame so does not run; future is cancelled below before calling shutdownNow
            future1620.cancel(false);
            recurringFuture70.cancel(false);
            
            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(Arrays.asList(1, 2, 2, 2),
                         TestRunnablesWithDefaultConstructor.numbers.stream()
                                                                    .map(AtomicInteger::get)
                                                                    .collect(Collectors.toList()));
            assertEquals(2, unfinishedRunnables.size()); // returns the cancelled tasks
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(4, unfinishedTasks.stream().count()); // 6-2 = 4 because 2 tasks were cancelled
            assertEquals(2, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isPeriodic).count());

            assertEquals(Arrays.asList("FirstRunnable", "SecondRunnable", "ThirdRunnable", "FirstRunnable"),
                         unfinishedTasks.stream()
                                        .sorted(Comparator.comparing(UnfinishedTasks.TaskInfo::getInitialDelayInNanos))
                                        .map(UnfinishedTasks.TaskInfo::getUnderlyingClass)
                                        .map(Class::getSimpleName)
                                        .collect(Collectors.toList()));
            
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(unfinishedTasks);
            oos.close();            
        }
        
        Thread.sleep(1000);
        assertEquals(Arrays.asList(1, 2, 2, 2),
                     TestRunnablesWithDefaultConstructor.numbers.stream()
                                                                .map(AtomicInteger::get)
                                                                .collect(Collectors.toList()));
        
        {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            SerializableScheduledExecutorService.UnfinishedTasks tasks = (SerializableScheduledExecutorService.UnfinishedTasks) ois.readObject();
            
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.importUnfinishedTasks(tasks);
            
            Thread.sleep(1000);
            // runnable1: 600 already done
            // runnable1: 1610 runs, then is done
            // runnable1: 1620 was cancelled above
            // runnable1: 2630 outside the time frame so does not run, pending at end of time frame
            // runnable2: 1650 runs, 2450 outside time frame so does not run, pending at end of time frame
            // runnable3: 1660+little runs, 2450+little outside time frame so does not run, pending at end of time frame
            // runnable4: 1670 was cancelled above
            
            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(Arrays.asList(2, 3, 3, 2),
                         TestRunnablesWithDefaultConstructor.numbers.stream()
                                                                    .map(AtomicInteger::get)
                                                                    .collect(Collectors.toList()));
            assertEquals(0, unfinishedRunnables.size());
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(3, unfinishedTasks.stream().count());
            assertEquals(2, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isPeriodic).count());
        }
    }


    
    private static class TestSerializableRunnable implements SerializableRunnable {
        private static final long serialVersionUID = 1L;

        private static final List<AtomicInteger> numbers = createNumbers(4); 

        private final int index;
        
        public TestSerializableRunnable(int index) {
            this.index = index;
        }
        
        public int getIndex() {
            return index;
        }
        
        @Override
        public void run() {
            numbers.get(index - 1).incrementAndGet();                
        }                    
    }

    @Test
    void testSerializableRunnable() throws InterruptedException, IOException, ClassNotFoundException, RecreateRunnableFailedException {
        Runnable runnableOne = new TestSerializableRunnable(1);
        Runnable runnableTwo = new TestSerializableRunnable(2);
        Runnable runnableThree = new TestSerializableRunnable(3);
        Runnable runnableFour = new TestSerializableRunnable(4);

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        {
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.schedule(runnableOne, 600, TimeUnit.MILLISECONDS);
            service.schedule(runnableOne, 1610, TimeUnit.MILLISECONDS);
            ScheduledFuture<?> future1620 = service.schedule(runnableOne, 1620, TimeUnit.MILLISECONDS); // cancelled below
            service.schedule(runnableOne, 2630, TimeUnit.MILLISECONDS);
            service.scheduleAtFixedRate(runnableTwo, 50, 800, TimeUnit.MILLISECONDS);
            service.scheduleWithFixedDelay(runnableThree, 60, 800, TimeUnit.MILLISECONDS);
            ScheduledFuture<?> recurringFuture70 = service.scheduleAtFixedRate(runnableFour, 70, 800, TimeUnit.MILLISECONDS); // cancelled below
            
            Thread.sleep(1000);
            // runnable1: 600 runs, then is done
            // runnable1: 1610 outside the time frame so does not run, pending at end of time frame
            // runnable1: 1620 outside the time frame so does not run; future is cancelled below before calling shutdownNow
            // runnable1: 2630 outside the time frame so does not run, pending at end of time frame
            // runnable2: 50 runs, 850 runs, 1650 outside time frame so does not run, pending at end of time frame
            // runnable3: 60 runs, 860+little runs, 1660+little outside time frame so does not run, pending at end of time frame
            // runnable4: 70 runs, 870 runs, 1670 outside time frame so does not run; future is cancelled below before calling shutdownNow
            future1620.cancel(false);
            recurringFuture70.cancel(false);

            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(Arrays.asList(1, 2, 2, 2), TestSerializableRunnable.numbers.stream().map(AtomicInteger::get).collect(Collectors.toList()));
            assertEquals(2, unfinishedRunnables.size()); // returns the cancelled tasks
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(4, unfinishedTasks.stream().count()); // 6-2 = 4 because 2 tasks were cancelled
            assertEquals(2, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isPeriodic).count());

            assertEquals(Arrays.asList(1, 2, 3, 1),
                    unfinishedTasks.stream()
                                   .sorted(Comparator.comparing(UnfinishedTasks.TaskInfo::getInitialDelayInNanos))
                                   .map(UnfinishedTasks.TaskInfo::getSerializableRunnable)
                                   .map(serializableRunnable -> (TestSerializableRunnable) serializableRunnable)
                                   .map(TestSerializableRunnable::getIndex)
                                   .collect(Collectors.toList()));

            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(unfinishedTasks);
            oos.close();
        }
        
        Thread.sleep(1000);
        assertEquals(Arrays.asList(1, 2, 2, 2), TestSerializableRunnable.numbers.stream().map(AtomicInteger::get).collect(Collectors.toList()));
        
        {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            SerializableScheduledExecutorService.UnfinishedTasks tasks = (SerializableScheduledExecutorService.UnfinishedTasks) ois.readObject();
            
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.importUnfinishedTasks(tasks);
            
            Thread.sleep(1000);
            // runnable1: 600 already done
            // runnable1: 1610 runs, then is done
            // runnable1: 1620 was cancelled above
            // runnable1: 2630 outside the time frame so does not run, pending at end of time frame
            // runnable2: 1650 runs, 2450 outside time frame so does not run, pending at end of time frame
            // runnable3: 1660+little runs, 2450+little outside time frame so does not run, pending at end of time frame
            // runnable4: 1670 was cancelled above

            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(Arrays.asList(2, 3, 3, 2), TestSerializableRunnable.numbers.stream().map(AtomicInteger::get).collect(Collectors.toList()));
            assertEquals(0, unfinishedRunnables.size());
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(3, unfinishedTasks.stream().count());
            assertEquals(2, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isPeriodic).count());
        }
    }
    



    private static class TestStandardCallable implements Callable<Integer> {
        private static AtomicInteger number = new AtomicInteger(); 

        public TestStandardCallable() {
        }
        
        @Override
        public Integer call() {
            return number.addAndGet(3);
        }
    }

    private static class TestSerializableCallable implements SerializableCallable<Integer> {
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger number = new AtomicInteger();

        private final AtomicInteger local; 

        public TestSerializableCallable(int initialValue) {
            local = new AtomicInteger(initialValue);
        }
        
        public int getLocal() {
            return local.get();
        }
        
        @Override
        public Integer call() {
            int result = local.incrementAndGet();
            number.set(result * 5);
            return result;
        }
    }
    
    @Test
    void testCallable() throws InterruptedException,
                               IOException, ClassNotFoundException, RecreateRunnableFailedException,
                               ExecutionException, TimeoutException {
        Callable<Integer> callableOne = new TestStandardCallable();
        Callable<Integer> callableTwo = new TestSerializableCallable(5);

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        {
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.schedule(callableOne, 610, TimeUnit.MILLISECONDS);
            service.schedule(callableOne, 1610, TimeUnit.MILLISECONDS);
            ScheduledFuture<Integer> future620 = service.schedule(callableTwo, 620, TimeUnit.MILLISECONDS);
            service.schedule(callableTwo, 9620, TimeUnit.MILLISECONDS);
            service.schedule(callableTwo, 8620, TimeUnit.MILLISECONDS);
            service.schedule(callableTwo, 1620, TimeUnit.MILLISECONDS);
            ScheduledFuture<Integer> future1630 = service.schedule(callableOne, 1630, TimeUnit.MILLISECONDS);

            Thread.sleep(1000);
            // callable1: 610 runs, then is done
            // callable1: 1610 outside time frame so does not run, pending at end of time frame
            // callable2: 620 runs, then is done
            // callable2: 9620 outside time frame so does not run, pending at end of time frame
            // callable2: 8620 outside time frame so does not run, pending at end of time frame
            // callable2: 1620 outside time frame so does not run, pending at end of time frame
            // callable1: 1630 outside time frame so does not run, but is cancelled below before shutdownNow is called
            future1630.cancel(false);

            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(Arrays.asList(1 * 3, 6 * 5), Arrays.asList(TestStandardCallable.number.get(), TestSerializableCallable.number.get()));
            assertEquals(1, unfinishedRunnables.size()); // returns the cancelled tasks
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(4, unfinishedTasks.stream().count());
            assertEquals(0, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isPeriodic).count());
            
            assertEquals(true, future620.isDone());
            assertEquals(false, future620.isCancelled());
            assertEquals(6, future620.get(1, TimeUnit.MILLISECONDS).intValue());
            assertEquals(6, future620.get().intValue());
            
            // future620 caused callableTwo.call to be run, thereby increasing TestSerializableCallable.number from 5 to 6
            assertEquals(Arrays.asList(6, 6, 6),
                    unfinishedTasks.stream()
                                   .sorted(Comparator.comparing(UnfinishedTasks.TaskInfo::getInitialDelayInNanos))
                                   .map(UnfinishedTasks.TaskInfo::getSerializableCallable)
                                   .filter(Objects::nonNull)
                                   .map(serializableCallable -> (TestSerializableCallable) serializableCallable)
                                   .map(TestSerializableCallable::getLocal)
                                   .collect(Collectors.toList()));

            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(unfinishedTasks);
            oos.close();            
        }
        
        Thread.sleep(1000);
        assertEquals(Arrays.asList(3, 30), Arrays.asList(TestStandardCallable.number.get(), TestSerializableCallable.number.get()));
        
        {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            SerializableScheduledExecutorService.UnfinishedTasks tasks = (SerializableScheduledExecutorService.UnfinishedTasks) ois.readObject();
            
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            Map<Class<?>, List<ScheduledFuture<?>>> futures
                    = service.importUnfinishedTasks(tasks,
                                                    Arrays.asList(TestSerializableCallable.class));
            assertEquals(1, futures.size());
            assertEquals(TestSerializableCallable.class, futures.keySet().iterator().next());
            assertEquals(3, futures.entrySet().iterator().next().getValue().size());
            
            @SuppressWarnings("unchecked")
            ScheduledFuture<Integer> future1620
                = (ScheduledFuture<Integer>) futures.entrySet()
                                                    .iterator()
                                                    .next()
                                                    .getValue().stream()
                                                               .collect(Collectors.minBy((lhs, rhs) -> Long.compare(lhs.getDelay(TimeUnit.MILLISECONDS),
                                                                                                                    rhs.getDelay(TimeUnit.MILLISECONDS))))
                                                               .get();
            
            Thread.sleep(1000);
            // callable1: 610 already done
            // callable1: 1610 runs, then done
            // callable2: 620 already done
            // callable2: 9620 outside time frame so does not run, pending at end of time frame
            // callable2: 8620 outside time frame so does not run, pending at end of time frame
            // callable2: 1620 runs, then done
            // callable1: 1630 was cancelled so not in this executor

            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(Arrays.asList(2 * 3, 7 * 5), Arrays.asList(TestStandardCallable.number.get(), TestSerializableCallable.number.get()));
            assertEquals(0, unfinishedRunnables.size());

            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(2, unfinishedTasks.stream().count());
            
            assertEquals(true, future1620.isDone());
            assertEquals(false, future1620.isCancelled());
            assertEquals(7, future1620.get(1, TimeUnit.MILLISECONDS).intValue());
            assertEquals(7, future1620.get().intValue());
            
        }
    }

    

    @SuppressWarnings("checkstyle:RightCurlyAlone")
    private static class TestNonPublicConstructorRunnable implements Runnable {
        TestNonPublicConstructorRunnable() { }

        @Override
        public void run() { }                    
    }

    @SuppressWarnings("checkstyle:RightCurlyAlone")
    private class TestNonStaticClassRunnable implements Runnable {
        public TestNonStaticClassRunnable() { }

        @Override
        public void run() { }                    
    }

    @SuppressWarnings("checkstyle:RightCurlyAlone")
    private static class TestNonPublicConstructorCallable implements Callable<Integer> {
        TestNonPublicConstructorCallable() { }

        @Override
        public Integer call() {
            return 7;
        }                    
    }

    @SuppressWarnings("checkstyle:RightCurlyAlone")
    private class TestNonStaticClassCallable implements Callable<Integer> {
        public TestNonStaticClassCallable() { }

        @Override
        public Integer call() {
            return 9;
        }                    
    }

    @Test
    @SuppressWarnings("checkstyle:RightCurlyAlone")
    void testNonSerializable() throws InterruptedException, IOException, ClassNotFoundException, RecreateRunnableFailedException {
        class LocalClassRunnable implements Runnable {
            public LocalClassRunnable() { }

            @Override
            public void run() {
            }                    
        }

        class LocalClassCallable implements Callable<Integer> {
            public LocalClassCallable() { }

            @Override
            public Integer call() {
                return 11;
            }                    
        }

        Runnable runnable1 = new TestNonPublicConstructorRunnable();
        Runnable runnable2 = new TestNonStaticClassRunnable();
        Runnable runnable3 = new LocalClassRunnable();
        Runnable runnable4 = () -> { };
        Callable<Integer> callable1 = new TestNonPublicConstructorCallable();
        Callable<Integer> callable2 = new TestNonStaticClassCallable();
        Callable<Integer> callable3 = new LocalClassCallable();
        Callable<Integer> callable4 = () -> 13;

        {
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            ((SerializableScheduledThreadPoolExecutor) service).logIfCannotSerialize(Level.WARNING);
            service.scheduleAtFixedRate(runnable1, 1410, 100, TimeUnit.MILLISECONDS);
            service.scheduleAtFixedRate(runnable2, 1420, 100, TimeUnit.MILLISECONDS);
            service.scheduleAtFixedRate(runnable3, 1430, 100, TimeUnit.MILLISECONDS);
            service.scheduleAtFixedRate(runnable4, 1440, 100, TimeUnit.MILLISECONDS);
            service.schedule(callable1, 1410, TimeUnit.MILLISECONDS);
            service.schedule(callable2, 1420, TimeUnit.MILLISECONDS);
            service.schedule(callable3, 1430, TimeUnit.MILLISECONDS);
            service.schedule(callable4, 1440, TimeUnit.MILLISECONDS);
            
            /* Example output:
             
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$TestNonPublicConstructorRunnable - no public constructor
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$TestNonStaticClassRunnable - no public constructor
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$1LocalClassRunnable - local class
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$$Lambda$31/1590550415 - synthetic class
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$TestNonPublicConstructorCallable - no public constructor
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$TestNonStaticClassCallable - no public constructor
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$1LocalClassCallable - local class
             Jul 17, 2018 8:59:04 AM myutils.util.concurrent.SerializableScheduledThreadPoolExecutor computeRunnableInfo
             WARNING: Cannot serialize myutils.util.concurrent.SerializableScheduledExecutorServiceTest$$Lambda$32/1058025095 - synthetic class
             
            */
            
            Thread.sleep(1000);
            
            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(8, unfinishedRunnables.size());
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(0, unfinishedTasks.stream().count());
        }
    }
    

    
    private static class TestRuntimeExceptionRunnable implements Runnable {
        private static final AtomicInteger number = new AtomicInteger();
        
        public TestRuntimeExceptionRunnable() {
            if (number.incrementAndGet() > 1) {
                throw new RuntimeException("runtime exception recreating Runnable");
            }
        }
        
        @Override
        public void run() {
        }                    
    }

    private static class TestCheckedExceptionRunnable implements Runnable {
        private static final AtomicInteger number = new AtomicInteger();
        
        public TestCheckedExceptionRunnable() throws Exception {
            if (number.incrementAndGet() > 1) {
                throw new Exception("checked exception recreating Runnable");
            }
        }
        
        @Override
        public void run() {
        }                    
    }

    private static class TestRuntimeExceptionCallable implements Callable<Integer> {
        private static final AtomicInteger number = new AtomicInteger();
        
        public TestRuntimeExceptionCallable() {
            if (number.incrementAndGet() > 1) {
                throw new RuntimeException("runtime exception recreating Callable");
            }
        }
        
        @Override
        public Integer call() {
            return 5;                
        }                    
    }

    @Test
    void testExceptionDeserialize() throws InterruptedException, IOException, ClassNotFoundException  {
        Runnable runnableOne = new TestRuntimeExceptionRunnable();
        Runnable runnableTwo;
        try {
            runnableTwo = new TestCheckedExceptionRunnable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Callable<Integer> callableOne = new TestRuntimeExceptionCallable();

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        {
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.scheduleAtFixedRate(runnableOne, 710, 800, TimeUnit.MILLISECONDS);
            service.scheduleAtFixedRate(runnableTwo, 720, 800, TimeUnit.MILLISECONDS);
            service.schedule(callableOne, 1530, TimeUnit.MILLISECONDS);
            
            Thread.sleep(1000);
            // runnable1: 710 runs, 1510 outside time range so does not run, pending at end of time frame
            // runnable2: 720 runs, 1520 outside time range so does not run, pending at end of time frame
            // callable1: 1530 outside time range so does not run, pending at end of time frame

            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(1, TestRuntimeExceptionRunnable.number.get());
            assertEquals(1, TestCheckedExceptionRunnable.number.get());
            assertEquals(1, TestRuntimeExceptionCallable.number.get());
            assertEquals(0, unfinishedRunnables.size());
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks();
            assertEquals(3, unfinishedTasks.stream().count());
            assertEquals(2, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isPeriodic).count());

            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(unfinishedTasks);
            oos.close();            
        }
        
        {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            SerializableScheduledExecutorService.UnfinishedTasks tasks = (SerializableScheduledExecutorService.UnfinishedTasks) ois.readObject();
            
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            RecreateRunnableFailedException exception = TestUtil.assertExceptionFromCallable(
                () -> { service.importUnfinishedTasks(tasks); return null; },
                RecreateRunnableFailedException.class);
            assertEquals(3, exception.getFailedClasses().size());
            assertEquals(Arrays.asList("TestCheckedExceptionRunnable", "TestRuntimeExceptionCallable", "TestRuntimeExceptionRunnable"),
                         exception.getFailedClasses().stream()
                                                     .map(Class::getSimpleName)
                                                     .sorted()
                                                     .collect(Collectors.toList()));
        }
    }

    
    
    private static class TestRunnableThatExceptionsOut implements SerializableRunnable {
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger staticCounter = new AtomicInteger();

        private final AtomicInteger index = new AtomicInteger();
        
        public TestRunnableThatExceptionsOut() {
        }
        
        public int getIndex() {
            return index.get();
        }
        
        @Override
        public void run() {
            if (staticCounter.incrementAndGet() >= 3) {
                throw new RuntimeException("runnable encountered exception");
            }
            index.incrementAndGet();
        }                    
    }
    
    @Test
    void testSerializeTaskThatExceptionedOut() throws InterruptedException, IOException, ClassNotFoundException, RecreateRunnableFailedException  {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        {
            TestRunnableThatExceptionsOut.staticCounter.set(0);
            TestRunnableThatExceptionsOut runnableOne = new TestRunnableThatExceptionsOut();
            
            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.scheduleAtFixedRate(runnableOne, 100, 200, TimeUnit.MILLISECONDS);
            
            Thread.sleep(1000);
            // runnable1: 100 runs, 300 runs, 500 encounters exception, 700 does not run, etc 

            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(3, TestRunnableThatExceptionsOut.staticCounter.get());
            assertEquals(2, runnableOne.getIndex());
            assertEquals(0, unfinishedRunnables.size());
            
            UnfinishedTasks unfinishedTasks = service.exportUnfinishedTasks(true);
            assertEquals(1, unfinishedTasks.stream().count());
            assertEquals(1, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isCompletedExceptionally).count());
            assertEquals(1, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::isPeriodic).count());
            assertEquals(0, unfinishedTasks.stream().filter(UnfinishedTasks.TaskInfo::wasInterrupted).count());

            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(unfinishedTasks);
            oos.close();            
        }
        
        {
            TestRunnableThatExceptionsOut.staticCounter.set(0);
            
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            SerializableScheduledExecutorService.UnfinishedTasks tasks = (SerializableScheduledExecutorService.UnfinishedTasks) ois.readObject();
            
            TestRunnableThatExceptionsOut anotherInstanceOfRunnableOne
                = tasks.stream()
                       .sorted(Comparator.comparing(UnfinishedTasks.TaskInfo::getInitialDelayInNanos))
                       .map(UnfinishedTasks.TaskInfo::getSerializableRunnable)
                       .findFirst()
                       .map(serializableRunnable -> (TestRunnableThatExceptionsOut) serializableRunnable)
                       .get();

            SerializableScheduledExecutorService service = MoreExecutors.newSerializableScheduledThreadPool(1);
            service.importUnfinishedTasks(tasks);
            
            Thread.sleep(1000);
            // runnable1: 1100 runs, 1300 runs, 1500 encounters exception, 1700 does not run, etc 

            List<Runnable> unfinishedRunnables = service.shutdownNow();
            assertEquals(3, TestRunnableThatExceptionsOut.staticCounter.get());
            assertEquals(4, anotherInstanceOfRunnableOne.getIndex());
            assertEquals(0, unfinishedRunnables.size());
        }
    }
}
