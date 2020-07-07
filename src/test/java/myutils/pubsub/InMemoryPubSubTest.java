package myutils.pubsub;

import static myutils.TestUtil.assertException;
import static myutils.TestUtil.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.ref.Cleaner;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import myutils.LogFailureToConsoleTestWatcher;
import myutils.TestUtil;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;



@ExtendWith(LogFailureToConsoleTestWatcher.class)
public class InMemoryPubSubTest {
    long startOfTime;
    
    @BeforeAll
    static void onStartAllTests() {
        System.out.println("start all tests");
        System.out.println("--------------------------------------------------------------------------------");
    }
    
    @AfterAll
    static void printAllTestsFinished() {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("all tests finished");
    }
    
    @BeforeEach
    void setStartOfTime(TestInfo testInfo) {
        startOfTime = System.currentTimeMillis();
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("test started: " + testInfo.getDisplayName());
    }
    
    @AfterEach
    void printTestFinished(TestInfo testInfo) {
        System.out.println("test finished: " + testInfo.getDisplayName());
    }

    
    ///////////////////////////////////////////////////////////////////////////////////////

    public static class CloneableString implements CloneableObject<CloneableString> {
        private static final long serialVersionUID = 1L;
        
        private String str;

        public CloneableString(String str) {
            this.str = str;
        }

        public String append(String text) {
            str += text;
            return str;
        }

        @Override
        public CloneableString clone() {
            try {
                return (CloneableString) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void testPublishAndSubscribeAndUnsubscribe() {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        Consumer<CloneableString> handleString1 = str -> words.add(str.append("-s1"));
        PubSub.Subscriber subscriber1 = pubSub.subscribe("hello", "Subscriber1", CloneableString.class, handleString1);
        Consumer<CloneableString> handleString2 = str -> words.add(str.append("-s2"));
        pubSub.subscribe("hello", "Subscriber2", CloneableString.class, handleString2);

        assertEquals("hello", publisher.getTopic());
        assertEquals("hello", subscriber1.getTopic());

        // test publish
        // since the handlers modify the messages, this also verifies that each subscriber handler gets a copy of the message
        words.clear();
        publisher.publish(new CloneableString("one"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("one-s1", "one-s2"));

        // test publish again
        words.clear();
        publisher.publish(new CloneableString("two"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("two-s1", "two-s2"));

        // test unsubscribe
        pubSub.unsubscribe(subscriber1);
        words.clear();
        publisher.publish(new CloneableString("three"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("three-s2"));

        // shows that new subscriber only handles messages after the time of subscription, not old messages
        Consumer<CloneableString> handleString3 = str -> words.add(str.append("-s3"));
        pubSub.subscribe("hello", "Subscriber3", CloneableString.class, handleString3);
        words.clear();
        pubSub.getPublisher("hello").ifPresent(samePublisher -> samePublisher.publish(new CloneableString("four")));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("four-s2", "four-s3"));
    }

    /**
     * In this test we create the subscriber before the publisher.
     * This could happen in a multi-threaded environment.
     */
    @Test
    void testSubscribeAndPublish() {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        Consumer<CloneableString> handleString1 = str -> words.add(str.append("-s1"));
        pubSub.subscribe("hello", "Subscriber1", CloneableString.class, handleString1);
        Consumer<CloneableString> handleString2 = str -> words.add(str.append("-s2"));
        pubSub.subscribe("hello", "Subscriber2", CloneableString.class, handleString2);
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);

        publisher.publish(new CloneableString("one"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("one-s1", "one-s2"));
    }
    
    /**
     * In this test we create the subscriber before the publisher.
     * This could happen in a multi-threaded environment.
     */
    @Test
    void testShutdown() {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        Consumer<CloneableString> handleString = str -> words.add(str.append(""));
        pubSub.subscribe("hello", "Subscriber1", CloneableString.class, handleString);
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);

        publisher.publish(new CloneableString("one"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("one"));
        
        pubSub.shutdown();
        
        publisher.publish(new CloneableString("two"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("one")); // no change as pubSub is shutdown
    }
    
    /**
     * In this test we publish two events, and while the first is running we unsubscribe.
     * Verify that the second event does not get processed.
     */
    @Test
    void testUnsubscribeWhileMessageInQueue() throws InterruptedException {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        Consumer<CloneableString> handleString1 = str -> {
            sleep(300);
            words.add(str.append(""));
        };
        PubSub.Subscriber subscriber = pubSub.subscribe("hello", "Subscriber", CloneableString.class, handleString1);

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.schedule(() -> {
            publisher.publish(new CloneableString("one"));
            publisher.publish(new CloneableString("two"));
        }, 0, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            pubSub.unsubscribe(subscriber);
        }, 250, TimeUnit.MILLISECONDS); 
        
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        
        sleep(400); // wait for subscribers to work
        assertThat(words, Matchers.contains("one"));
    }
    
    /**
     * In this test we have many threads.
     */
    @Test
    void testMultipleThreads() {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        Instant startTime = Instant.now();
        AtomicReference<Instant> endTime = new AtomicReference<>();
        PubSub pubSub = new InMemoryPubSub(cleaner, 3, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        IntStream.rangeClosed(1, 5).forEach(i -> {
            pubSub.subscribe("hello", "Subscriber" + i,
                CloneableString.class,
                str -> {
                    System.out.println("currentThread=" + Thread.currentThread().getName() + ", i=" + i);
                    sleep(20 * i);
                    String word = str.append("-s" + i);
                    words.add(word);
                    System.out.println("currentThread=" + Thread.currentThread().getName() + ", i=" + i + ", word=" + word);
                    endTime.set(Instant.now());
                });
        });

        publisher.publish(new CloneableString("one"));
        publisher.publish(new CloneableString("two"));
        publisher.publish(new CloneableString("three"));
        
        // subscriber1 takes 20ms to run
        // subscriber2 takes 40ms to run
        // etc, so all 5 subscribers take 20 + 40 + 60 + 80 + 100 = 300ms
        // as we publish 3 events, this should take about 900ms if there were 1 thread
        // but as we have 3 threads, it will take closer to 300ms
        sleep(340); // wait for subscribers to work
        System.out.println("timeTaken=" + Duration.between(startTime, endTime.get())); // typical output: timeTaken=PT0.314515S
        
        System.out.println(words);
        assertThat(words,
                Matchers.not(Matchers.contains("one-s1", "one-s2", "one-s3", "one-s4", "one-s5",
                                              "two-s1", "two-s2", "two-s3", "two-s4", "two-s5",
                                              "three-s1", "three-s2", "three-s3", "three-s4", "three-s5")));
        assertThat(words,
                Matchers.containsInAnyOrder("one-s1", "one-s2", "one-s3", "one-s4", "one-s5",
                                            "two-s1", "two-s2", "two-s3", "two-s4", "two-s5",
                                            "three-s1", "three-s2", "three-s3", "three-s4", "three-s5"));
    }
    
    /**
     * In one parameterized version of this test we set up a PubSub system with a priority queue such that messages published to Subscriber1 are processed first.
     */
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = {"default", "priority"})
    void testPrioritySubscribers(String queueType) {
        Cleaner cleaner = Cleaner.create();
        Supplier<Queue<PubSub.Subscriber>> queueCreator;
        switch (queueType) {
            case "default": queueCreator = PubSub.defaultQueueCreator(); break;
            case "priority": queueCreator = () -> new PriorityQueue<>(Comparator.comparing(PubSub.Subscriber::getSubscriberName)); break;
            default: throw new UnsupportedOperationException();
        }
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, queueCreator, PubSub.defaultSubscriptionMessageExceptionHandler());
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        Consumer<CloneableString> handleString1 = str -> {
            sleep(50);
            words.add(str.append("-s1"));
        };
        Consumer<CloneableString> handleString2 = str -> {
            sleep(50);
            words.add(str.append("-s2"));
        };
        Consumer<CloneableString> handleString3 = str -> {
            sleep(50);
            words.add(str.append("-s3"));
        };
        pubSub.subscribe("hello", "Subscriber1", CloneableString.class, handleString1);
        pubSub.subscribe("hello", "Subscriber2", CloneableString.class, handleString2);
        pubSub.subscribe("hello", "Subscriber3", CloneableString.class, handleString3);

        publisher.publish(new CloneableString("one"));
        publisher.publish(new CloneableString("two"));
        publisher.publish(new CloneableString("three"));
        
        sleep(850); // wait for subscribers to work
        System.out.println(words);
        switch (queueType) {
            case "default":
                assertThat(words, Matchers.contains("one-s1", "one-s2", "one-s3", "two-s1", "two-s2", "two-s3", "three-s1", "three-s2", "three-s3"));
                break;
            case "priority": 
                assertThat(words, Matchers.contains("one-s1", "two-s1", "three-s1", "one-s2", "two-s2", "three-s2", "one-s3", "two-s3", "three-s3"));
                break;
            default: throw new UnsupportedOperationException();
        }
        
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    // In these tests the leaf classes implement CloneableObject

    public interface Animal {
    }

    public static final class Cat implements Animal, CloneableObject<Cat> {
        private static final long serialVersionUID = 1L;

        private final int lengthOfTailInInches;

        public Cat(int lengthOfTailInInches) {
            this.lengthOfTailInInches = lengthOfTailInInches;
        }

        public int getLengthOfTailInInches() {
            return lengthOfTailInInches;
        }

        @Override
        public final Cat clone() {
            try {
                return (Cat) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class Frog implements Animal, CloneableObject<Frog> {
        private static final long serialVersionUID = 1L;

        private final int loudnessOfCroak;

        public Frog(int loudnessOfCroak) {
            this.loudnessOfCroak = loudnessOfCroak;
        }

        public int getLoudnessOfCroak() {
            return loudnessOfCroak;
        }

        @Override
        public final Frog clone() {
            try {
                return (Frog) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void testPublishAndSubscribeWithInheritance1() {
        Cleaner cleaner = Cleaner.create();
        List<Integer> words = Collections.synchronizedList(new ArrayList<>());
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        PubSub.Publisher publisher = pubSub.createPublisher("hello", Animal.class);
        Consumer<Cat> handleCat = cat -> words.add(cat.getLengthOfTailInInches());
        pubSub.subscribe("hello", "CatSubscriber", Cat.class, handleCat);
        Consumer<Frog> handleFrog = frog -> words.add(frog.getLoudnessOfCroak());
        pubSub.subscribe("hello", "FrogSubscriber", Frog.class, handleFrog);

        publisher.publish(new Cat(12));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains(12));
        publisher.publish(new Frog(3));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains(12, 3));
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    // In these classes the base class implements CloneableObject

    public abstract static class Base implements CloneableObject<Base> {
        private static final long serialVersionUID = 1L;

        @Override
        public final Base clone() {
            try {
                return (Base) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class Derived1 extends Base {
        private static final long serialVersionUID = 1L;

        private final String string;

        public Derived1(String string) {
            this.string = string;
        }

        @Override
        public String toString() {
            return string;
        }
    }

    public static final class Derived2 extends Base {
        private static final long serialVersionUID = 1L;

        private final int integer;

        public Derived2(int integer) {
            this.integer = integer;
        }

        @Override
        public String toString() {
            return Integer.toString(integer);
        }
    }

    @Test
    void testPublishAndSubscribeWithInheritance2() {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        PubSub.Publisher publisher = pubSub.createPublisher("hello", Base.class);
        Consumer<Base> handleBase = base -> words.add(base.toString());
        pubSub.subscribe("hello", "BaseSubscriber", Base.class, handleBase);

        publisher.publish(new Derived1("fifteen"));
        publisher.publish(new Derived2(15));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("fifteen", "15"));
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    // Test errors

    @Test
    void testErrorOnCreatePublisherTwice() {
        Cleaner cleaner = Cleaner.create();
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        pubSub.createPublisher("hello", CloneableString.class);
        assertException(() -> pubSub.createPublisher("hello", CloneableString.class), IllegalArgumentException.class, "publisher already exists: hello");
    }

    @Test
    @SuppressWarnings("checkstyle:LineLength")
    void testErrorOnSubscriberWrongClassType() {
        Cleaner cleaner = Cleaner.create();
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        pubSub.createPublisher("hello", CloneableString.class);
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        Consumer<Base> handleBase = base -> words.add(base.toString());
        assertException(() -> pubSub.subscribe("hello", "Subscriber", Base.class, handleBase), IllegalArgumentException.class, "subscriber class must be the same as or inherit from the publisher class: publisherClass=myutils.pubsub.InMemoryPubSubTest$CloneableString subscriberClass=myutils.pubsub.InMemoryPubSubTest$Base");
    }

    @Test
    void testExceptionOnSubscriberCallback1() {
        Cleaner cleaner = Cleaner.create();
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler());
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        Consumer<CloneableString> handleString = str -> {
            throw new RuntimeException("Test Exception");
        };
        pubSub.subscribe("hello", "Subscriber", CloneableString.class, handleString);

        publisher.publish(new CloneableString("one"));
        sleep(100); // wait for subscribers to work
        // verify that console log shows something like this:
        /*
        WARNING: Exception invoking subscriber
        java.lang.RuntimeException: Test Exception
        at myutils.util.concurrent.InMemoryPubSubTest.lambda$testExceptionOnSubscriberCallback$12(InMemoryPubSubTest.java:243)
        */
    }

    public static class TestEvent implements CloneableObject<TestEvent> {
        private static final long serialVersionUID = 1L;

        private int retryCount;

        public TestEvent() {
        }
        
        int getRetryCount() {
            return retryCount;
        }
        
        void incrementRetryCount() {
            retryCount++;
        }

        @Override
        public TestEvent clone() {
            try {
                return (TestEvent) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Demonstrate how to retry failed subscription handlers.
     */
    @Test
    void testExceptionOnSubscriberCallback2() {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        var mySubscriptionMessageExceptionHandler = new InMemoryPubSub.SubscriptionMessageExceptionHandler() {
            @Override
            public void handleException(PubSub.Subscriber subscriber, CloneableObject<?> message, Throwable e) {
                if (message instanceof TestEvent) {
                    TestEvent event = (TestEvent) message;
                    event.incrementRetryCount();
                }
                words.add(subscriber.getSubscriberName() + "-" + message.getClass().getSimpleName() + " : " + e.getMessage());
                subscriber.addMessage(message);
            }
        };
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), mySubscriptionMessageExceptionHandler);
        PubSub.Publisher publisher = pubSub.createPublisher("hello", TestEvent.class);
        Consumer<TestEvent> handleEvent = event -> {
            if (event.getRetryCount() == 0) {
                throw new RuntimeException("Test Exception");
            } else {
                words.add("success");
            }
        };
        pubSub.subscribe("hello", "SubscriberThatThrows", TestEvent.class, handleEvent);

        publisher.publish(new TestEvent());
        sleep(100); // wait for subscribers to work
        sleep(100); // wait for message handler to work
        assertThat(words,
                   Matchers.contains("SubscriberThatThrows-TestEvent : Test Exception",
                                     "success"));
    }

    /**
     * In this test we retry a failed a subscription handler but insert an object of the wrong type into the subscriber queue, resulting in an exception.
     */
    @Test
    void testExceptionOnSubscriberCallback3() {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        var mySubscriptionMessageExceptionHandler = new InMemoryPubSub.SubscriptionMessageExceptionHandler() {
            @Override
            public void handleException(PubSub.Subscriber subscriber, CloneableObject<?> message, Throwable e) {
                if (message instanceof TestEvent) {
                    TestEvent event = (TestEvent) message;
                    event.incrementRetryCount();
                }
                words.add(subscriber.getSubscriberName() + "-" + message.getClass().getSimpleName() + " : " + e.getMessage());
                try {
                    subscriber.addMessage(new CloneableString("wrong type - expecting TestEvent"));
                } catch (IllegalArgumentException e2) {
                    words.add(e2.getMessage());
                }
            }
        };
        PubSub pubSub = new InMemoryPubSub(cleaner, 1, PubSub.defaultQueueCreator(), mySubscriptionMessageExceptionHandler);
        PubSub.Publisher publisher = pubSub.createPublisher("hello", TestEvent.class);
        Consumer<TestEvent> handleEvent = event -> {
            if (event.getRetryCount() == 0) {
                throw new RuntimeException("Test Exception");
            } else {
                words.add("success");
            }
        };
        pubSub.subscribe("hello", "SubscriberThatThrows", TestEvent.class, handleEvent);

        publisher.publish(new TestEvent());
        sleep(100); // wait for subscribers to work
        sleep(100); // wait for message handler to work
        assertThat(words,
                   Matchers.contains("SubscriberThatThrows-TestEvent : Test Exception",
                                     "message must be or inherit from myutils.pubsub.InMemoryPubSubTest$TestEvent"));
    }
}
