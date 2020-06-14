package myutils.util.concurrent;

import static myutils.TestUtil.assertException;
import static myutils.TestUtil.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;


public class InMemoryPubSubTest {
    ///////////////////////////////////////////////////////////////////////////////////////

    public static class CloneableString implements InMemoryPubSub.CloneableObject<CloneableString> {
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
    void testPublishAndSubscribe() {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        InMemoryPubSub pubSub = new InMemoryPubSub(1, InMemoryPubSub.defaultSubscriptionEventExceptionHandler());
        InMemoryPubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        Consumer<CloneableString> handleString1 = str -> words.add(str.append("-1"));
        InMemoryPubSub.Subscriber subscriber1 = pubSub.subscribe("hello", "Subscriber1", CloneableString.class, handleString1);
        Consumer<CloneableString> handleString2 = str -> words.add(str.append("-2"));
        pubSub.subscribe("hello", "Subscriber2", CloneableString.class, handleString2);

        assertEquals("hello", publisher.getTopic());
        assertEquals("hello", subscriber1.getTopic());

        // test publish
        // since the handlers modify the events, this also verifies that each subscriber handler gets a copy of the event
        words.clear();
        publisher.publish(new CloneableString("one"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("one-1", "one-2"));

        // test publish again
        words.clear();
        publisher.publish(new CloneableString("two"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("two-1", "two-2"));

        // test unsubscribe
        pubSub.unsubscribe(subscriber1);
        words.clear();
        publisher.publish(new CloneableString("three"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("three-2"));

        // shows that new subscriber only handles events after the time of subscription, not old events
        Consumer<CloneableString> handleString3 = str -> words.add(str.append("-3"));
        pubSub.subscribe("hello", "Subscriber3", CloneableString.class, handleString3);
        words.clear();
        pubSub.getPublisher("hello").ifPresent(samePublisher -> samePublisher.publish(new CloneableString("four")));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("four-2", "four-3"));
    }

    @Test
    void testSubscribeAndPublish() {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        InMemoryPubSub pubSub = new InMemoryPubSub(1, InMemoryPubSub.defaultSubscriptionEventExceptionHandler());
        Consumer<CloneableString> handleString1 = str -> words.add(str.append("-1"));
        pubSub.subscribe("hello", "Subscriber1", CloneableString.class, handleString1);
        Consumer<CloneableString> handleString2 = str -> words.add(str.append("-2"));
        pubSub.subscribe("hello", "Subscriber2", CloneableString.class, handleString2);
        InMemoryPubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);

        publisher.publish(new CloneableString("one"));
        sleep(100); // wait for subscribers to work
        assertThat(words, Matchers.contains("one-1", "one-2"));
    }

    ///////////////////////////////////////////////////////////////////////////////////////
    // In these tests the leaf classes implement CloneableObject

    public interface Animal {
    }

    public static final class Cat implements Animal, InMemoryPubSub.CloneableObject<Cat> {
        private final int lengthOfTailInInches;

        public Cat(int lengthOfTailInInches) {
            this.lengthOfTailInInches = lengthOfTailInInches;
        }

        public int getLengthOfTailInInches() {
            return lengthOfTailInInches;
        }

        @Override
        public Cat clone() {
            try {
                return (Cat) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class Frog implements Animal, InMemoryPubSub.CloneableObject<Frog> {
        private final int loudnessOfCroak;

        public Frog(int loudnessOfCroak) {
            this.loudnessOfCroak = loudnessOfCroak;
        }

        public int getLoudnessOfCroak() {
            return loudnessOfCroak;
        }

        @Override
        public Frog clone() {
            try {
                return (Frog) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void testPublishAndSubscribeWithInheritance1() {
        List<Integer> words = Collections.synchronizedList(new ArrayList<>());
        InMemoryPubSub pubSub = new InMemoryPubSub(1, InMemoryPubSub.defaultSubscriptionEventExceptionHandler());
        InMemoryPubSub.Publisher publisher = pubSub.createPublisher("hello", Animal.class);
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

    public abstract static class Base implements InMemoryPubSub.CloneableObject<Base> {
        @Override
        public Base clone() {
            try {
                return (Base) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final class Derived1 extends Base {
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
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        InMemoryPubSub pubSub = new InMemoryPubSub(1, InMemoryPubSub.defaultSubscriptionEventExceptionHandler());
        InMemoryPubSub.Publisher publisher = pubSub.createPublisher("hello", Base.class);
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
        InMemoryPubSub pubSub = new InMemoryPubSub(1, InMemoryPubSub.defaultSubscriptionEventExceptionHandler());
        pubSub.createPublisher("hello", CloneableString.class);
        assertException(() -> pubSub.createPublisher("hello", CloneableString.class), IllegalArgumentException.class, "publisher already exists: hello");
    }

    @Test
    @SuppressWarnings("checkstyle:LineLength")
    void testErrorOnSubscriberWrongClassType() {
        InMemoryPubSub pubSub = new InMemoryPubSub(1, InMemoryPubSub.defaultSubscriptionEventExceptionHandler());
        pubSub.createPublisher("hello", CloneableString.class);
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        Consumer<Base> handleBase = base -> words.add(base.toString());
        assertException(() -> pubSub.subscribe("hello", "Subscriber", Base.class, handleBase), IllegalArgumentException.class, "subscriber class must be the same as or inherit from the publisher class: publisherClass=myutils.util.concurrent.InMemoryPubSubTest$CloneableString subscriberClass=myutils.util.concurrent.InMemoryPubSubTest$Base");
    }

    @Test
    void testExceptionOnSubscriberCallback1() {
        InMemoryPubSub pubSub = new InMemoryPubSub(1, InMemoryPubSub.defaultSubscriptionEventExceptionHandler());
        InMemoryPubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
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

    @Test
    void testExceptionOnSubscriberCallback2() {
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        var myEventSubscriptionEventExceptionHandler = new InMemoryPubSub.SubscriptionEventExceptionHandler() {
            @Override
            public void handleException(InMemoryPubSub.Subscriber subscriber, InMemoryPubSub.CloneableObject<?> event, Throwable e) {
                errors.add(subscriber.getSubscriberName() + "-" + event.getClass().getSimpleName() + " : " + e.getMessage());
            }
        };
        InMemoryPubSub pubSub = new InMemoryPubSub(1, myEventSubscriptionEventExceptionHandler);
        InMemoryPubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        Consumer<CloneableString> handleString = str -> {
            throw new RuntimeException("Test Exception");
        };
        pubSub.subscribe("hello", "SubscriberThatThrows", CloneableString.class, handleString);

        publisher.publish(new CloneableString("one"));
        sleep(100); // wait for subscribers to work
        assertThat(errors, Matchers.contains("SubscriberThatThrows-CloneableString : Test Exception"));
    }
}
