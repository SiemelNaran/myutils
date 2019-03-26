myutils
=======

Utilities for Java. Shell script utilities.


License
-------
This code is under the [Apache Licence v2](https://www.apache.org/licenses/LICENSE-2.0).


Java Classes
------------

- *PageList*: Class representing an array as an array of pages.

- *SerializableScheduledExecutorService*: Interface extending ScheduledExecutorService that allows the scheduled executions to be serialized.

- *StackTraceCompletableFuture*: Replacement for CompletableFuture that adds a layer to remember the call stack of the place that created the completed future. This is useful for debugging.


Shell scripts
-------------

- *findTokens.sh*: Find tokens (or words) in a file starting with or containing text.

- *multifile.sh*: Copy files that contain <sourceSubstring> to a similar filename where <sourceSubstring> is replaced by <dest>.


Running tests
-------------

The tests only pass on Java 8.  If you have many versions of Java, remember to specify the Java version, for example:

```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/
mvn clean test
```
