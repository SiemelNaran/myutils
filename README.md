myutils
=======

Utilities for Java.


License
-------
This code is under the [Apache Licence v2](https://www.apache.org/licenses/LICENSE-2.0).


Classes
-------

- *PageList*: Class representing an array as an array of pages.

- *SerializableScheduledExecutorService*: Interface extending ScheduledExecutorService that allows the scheduled executions to be serialized.

- *StackTraceCompletableFuture*: Replacement for CompletableFuture that adds a layer to remember the call stack of the place that created the completed future. This is useful for debugging.
