myutils
=======

Utilities for Java.

A publish-subscribe framework in Java.

An expression parser framework in Java.

Miscellaneous shell script utilities.


License
-------
This code is under the [Apache Licence v2](https://www.apache.org/licenses/LICENSE-2.0).


Building
--------

Install the following:
- Maven 3.6.3
- Java 13

To build all projects `mvn clean install`.
To build all projects skipping tests `mvn clean install -DskipTests`.


Maven
-----

To delete the files in the local repository
```
rm -rfv ~/.m2/repository/org/sn/myutils/
```


Eclipse IDE
-----------

Eclipse 2020-03.

Steps:
- Optional: Ensure that .project and .classpath files are deleleted
```
find ../myutils/ -name "*.classpath" -exec rm -rfv {} \;
find ../myutils/ -name "*.project" -exec rm -rfv {} \;
```
- File -> Import -> Maven -> Existing Maven Projects
- Pick the myutils folder
- Ensure all checkboxes are checked and click Finish


IntelliJ IDE
------------

IntelliJ 2020.2.

Steps:
- Optional: Ensure that all .iml and the .idea folder are deleted.
  If you get incomprehensible compile errors about java.util classes not being found, invalidate cache and restart.
```
find ../myutils/ -name "*.iml" -exec rm -rfv {} \;
rm -rfv ../myutils/.idea/
```
- Open or Import
- Pick the myutils folder and click OK

Java Classes
------------

The main part of this project is the Java classes.

### Utility Classes 

There are a bunch of utility classes that extend the JVM.
To use ensure your module-info.java looks something like this:

```
module temp {
    requires org.sn.myutils.core;
}
```


- *AdaptingIterator*: Class to map the value returned by an iterator into another value. Use only if stream() with map() is not possible.

- *PageList*: Class representing an array as an array of pages.
    - Inserting an element in the middle of the array is fast as we only have to insert an element in one page.
    - There is also a specialization of binary search in MoreCollections.
    - PageList is just an interface.  There are two types of page lists: ArrayPageList and LinkedListPageList, meaning that each page is either an ArrayList or a LinkedList.

- *MoreCollections*: Various enhancements to java.util.Collections.
    - There is a binary search function to search a list, but applying a function to each element.
      For example if you have a list of contacts sorted by first name, you can search for a contact by first name.
    - There is a specialization of binary search for PageList's.
    - There is a function to add a probably larger element to an already sorted list, resulting in O(1) performance in theory.

- *MoreCollectors*: Various enhancements to java.util.stream.Collectors.
    - There is a function minAndMaxBy to find min and max in one pass.
    - There is a function maxBy to find the N largest elements.
    - There is a function minBy to find the N smallest elements.

- *MultimapUtils*: A way to deal with multimap, as an alternative to classes like com.google.common.collect.ListMultimap. Example usage:
```
    Map<Key, List<Value>> multimap = new HashMap<>();
    MultimapUtils<Key, Value> multimap = new MultimapUtils<>(multimap, ArrayList::new);
    multimap.put(new Key("key"), new Value(5));
    multimap.put(new Key("key"), new Value(7));
```

- *RewindableIterator*: An iterator that has a rewind function to let you go back one element.
It does not let you rewind any number of times, as that would basically be a list iterator.

- *SimpleStringTokenizerFactory*: A string tokenizer more powerful than the basic StringTokenizer as it can read all of the characters within quotes as one token, and much more.

- *Trie*: A basic trie data structure.
    - Trie is just an interface.  There are two types of tries: SimpleTrie and SpaceEfficientTrie.
    - In SimpleTrie, each node is one character.
    - In SpaceEfficientTrie each node can be many characters if there are no child nodes with other prefixes.
    - Trie implements the Map interface.

Suppose trie contains "bottom","bottle", and "bottles".

```
SimpleTrie:
     ROOT
      |
      b
      |
      o
      |
      t
      |
      t
     / \
    o   l
    |   |
    m*  e*
        |
        s*

```

```
SpaceEfficientTrie:
     ROOT
      |
     bott
     / \
   om*  le*
         |
         s*

```

- *WeightedRandom*: Given a list of weights, find a random number respecting those weights.

- *ZipMinIterator*: An iterator that takes a list of iterators over sorted ranges and returns the next highest element.


### Concurrent classes

- *CompletionStageUtils*: Utilities for completion stages.
    - Given a list of completion stages, return a single completion stage whose result is a list of the value of each completion stage.

- *HashLocks*: A class that provides a fixed number of locks, and locking an object will find the hash code of the object and then lock the appropriate lock/ This way we can safely lock on any string. There is always the risk of collisions, so for debugging purposes you can enable collision tracking, which tracks how many different strings map to the same lock.

```
    var locks = HashLocks.create(3, () -> new ReentrantLock(false), (reentrantLock, setString) -> reentrantLock.isLocked());
    Lock lock0 = locks.getLock(0);
    Lock lock1 = locks.getLock(1);
    Lock lock2 = locks.getLock(2);
    assertEquals(ReentrantLock.class, lock0.getClass());
    assertNotSame(lock0, lock1);
    assertNotSame(lock0, lock2);
    assertNotSame(lock0, lock2);
```

- *PriorityLock*: A lock which grants flow to the thread with the highest priority. In other words, if two threads are waiting for the same lock, the thread with the higher priority will get access first when the lock is available. Similarly, the await functions signal the thread with the highest priority first.

- *PriorityExecutorService*: An executor service that has an additional submit function that takes a priority. Jobs with higher priority run first. When calling the function submit that takes only one argument (the callable or runnable) then the `Thread.NORM_PRIORITY` or 5 is used. The class implementing this interface is PriorityThreadPoolExecutor.

```
public interface PriorityExecutorService extends ExecutorService {
    Future<?> submit(int priority, Runnable task);
}
```

- *SerializableScheduledExecutorService*: An executor service that allows the tasks in the service to be serialized, so that on shutdown we can save the tasks that have not started, and on restart we can restore the service to those future tasks.
    - The class implementing this interface is SerializableScheduledThreadPoolExecutor.
    - Regular Runnable and Callable classes are not serializable. One must use SerializableRunnable and SerializableCallable.

- *StackTraceCompletableFuture*: Replacement for CompletableFuture that adds a layer to remember the call stack of the place that created the completed future. This is useful for debugging. An example call stack is below:
    - Related class CompletableFutureFactory can be used to generate either CompletableFuture or StackTraceCompletableFuture. The default is CompletableFuture.
    - Related class StackTraces for controlling the look of the output stack trace. For example you can ignore lines originating from "org.eclipse", "org.junit", "sun.reflect".

```
org.sn.myutils.util.concurrent.StackTraces$StackTracesCompletionException: java.lang.IllegalStateException: failed
	at org.sn.myutils.util.concurrent.StackTraces.generateException(StackTraces.java:114)
	at org.sn.myutils.util.concurrent.StackTraceCompletableFuture.lambda$0(StackTraceCompletableFuture.java:53)
Caused by: java.lang.IllegalStateException: failed
	at org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.lambda$common$3(StackTraceCompletableFutureTest.java:69)
	... 8 more
Called from
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.common(StackTraceCompletableFutureTest.java:65)
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.testExceptionalExecutionJoin(StackTraceCompletableFutureTest.java:161)
Called from
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.doEvenMore(StackTraceCompletableFutureTest.java:38)
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.doMore(StackTraceCompletableFutureTest.java:46)
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.common(StackTraceCompletableFutureTest.java:63)
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.testExceptionalExecutionJoin(StackTraceCompletableFutureTest.java:161)
Called from
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.common(StackTraceCompletableFutureTest.java:57)
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.testExceptionalExecutionJoin(StackTraceCompletableFutureTest.java:161)
Called from
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.common(StackTraceCompletableFutureTest.java:51)
	org.sn.myutils.util.concurrent.StackTraceCompletableFutureTest.testExceptionalExecutionJoin(StackTraceCompletableFutureTest.java:161)
```

- *TestScheduledThreadPoolExecutor*: A scheduled thread pool executor to be used for testing. You can schedule a task to run in the future, say in 10 minutes from now, and then call a function advanceTime to advance time by 10 minutes and verify that the task was called.

- *TimedReentrantLock*: An ReentrantLock that has additional functions to track the usage of the lock. You can get total wait time, total lock running time, and total idle time. TimedReentrantLock can be used inside HashLocks.


### Parser classes

There is a class ExpressionParser that parses expressions, such as arithmetic expressions.
One provides things like a list of binary operators, a list of unary operators, a list of known function names, etc and the code will build a parse tree and evaluate any expression as needed.

There is also a method to parse numbers with units, so you can do stuff like "1cm + 2in".

```
    @Test
    void testEvaluateSymbols() throws ParseException {
        Map<String, Object> scope = new HashMap<>();
        scope.put("x", 3);
        scope.put("y", 4);
        assertEquals(14, evaluate("2+x*y", scope));
    }

    private static int evaluate(String expression, Map<String, Object> scope) throws ParseException {
        ParseNode tree = PARSER.parse(expression);
        Map<String, Class<?>> scopeTypes = scope.entrySet()
                                                .stream()
                                                .collect(Collectors.toMap(Map.Entry::getKey,
                                                                          entry -> entry.getValue().getClass()));
        assertEquals(Integer.class, tree.checkEval(scopeTypes));
        return (int) tree.eval(scope);
    }

    private static final ExpressionParser PARSER = ExpressionParser.builder()
                                                                   .setNumberFactory(NUMBER_FACTORY)
                                                                   .addBinaryOperator(PLUS.class)
                                                                   .addBinaryOperator(MINUS.class)
                                                                   .addBinaryOperator(TIMES.class)
                                                                   .addBinaryOperator(DIVIDE.class)
                                                                   .addUnaryOperator(POSITIVE.class)
                                                                   .addUnaryOperator(NEGATIVE.class)
                                                                   .setFunctionCase(StringCase.ALL_LETTERS_SAME_CASE)
                                                                   .addFunction(MAX.class)
                                                                   .addFunction(MIN.class)
                                                                   .build();
```

Please consider alternatives such as javacc and antlr for greater flexibility.


### Publish-Subscribe classes

There are two publish-subscribe implementations. The first is an in-memory publish-subscribe, where the publisher and subscribers all reside in one JVM. The second is a distributed publish-subscribe, where the publisher and subscribers reside in different JVM's (and most likely on different machines). The distributed publish-subscribe provides a central server, which all clients communicate with.

Here is an example of the in-memory pubsub:

```
    @Test
    void testPublishAndSubscribeAndUnsubscribe() {
        List<String> words = Collections.synchronizedList(new ArrayList<>());

        PubSub pubSub = new InMemoryPubSub(new PubSub.PubSubConstructorArgs(1, PubSub.defaultQueueCreator(), PubSub.defaultSubscriptionMessageExceptionHandler()));
        PubSub.Publisher publisher = pubSub.createPublisher("hello", CloneableString.class);
        PubSub.Subscriber subscriber1 = pubSub.subscribe("hello", "Subscriber1", CloneableString.class, str -> words.add(str.append("-s1")));
        pubSub.subscribe("hello", "Subscriber2", CloneableString.class, str -> words.add(str.append("-s2")));

        publisher.publish(new CloneableString("one"));
        assertThat(words, Matchers.contains("one-s1", "one-s2"));
        ...
        ...
        ...
    }
```

The distributed publish-subscribe is built on top of the in-memory publish-subscribe, and its architecture is as follows:
- When a client becomes a publisher, it sends its publisher to the central server.
- When another client subscribes, the central server sends the publisher over to this other client. So it's as if each client has a replica of the in-memory publish-subscribe.
- When the first client publishes a message, it gets relayed to the other client, and it's as if someone called pubsub.publish on the other client.

Communication between client and server happens via sockets. The client class is DistributedSocketPubSub and the central server is DistributedMessageServer.
The server class does not have the word 'Socket' in its name as in the future the server may also handle HTTP requests.

Here is an example of the distributed pubsub:

```
    @Test
    void testSubscribeAndPublishAndUnsubscribe() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());

        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001
        );
        startFutures.add(client1.startAsync());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.startAsync());

        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   30003
        );
        startFutures.add(client3.startAsync());

        waitFor(startFutures);

        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        Subscriber subscriber2a = client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        Subscriber subscriber2b = client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        client3.subscribe("hello", "ClientThreeSubscriber", CloneableString.class, str -> words.add(str.append("-s3")));
        sleep(250); // time to let subscribers be sent to server

        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);

        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish(), and similarly for client3
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2a", "one-s2b", "two-s2a", "two-s2b", "one-s3", "two-s3"));
        ...
        ...
        ...
    }
```


Shell scripts
-------------

- *findTokens.sh*: Find tokens (or words) in a file starting with or containing the specified text.
For example, if you have a bunch of files with strings like `PRIORITY_LOW`, `PRIORITY_MEDIUM`, `PRIORITY_HIGH` in them,
call findTokens.sh searching for `PRIORITY_`.

- *multifile.sh*: Copy files that contain <sourceSubstring> to a similar filename where <sourceSubstring> is replaced by <dest>.
For example, use this file to copy "hello.txt" and "hello.java" to "world.txt" and "world.java".
