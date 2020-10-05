package org.sn.myutils.pubsub;

import java.lang.System.Logger.Level;
import org.junit.jupiter.api.Test;


public class LoggingLevelTest {
    private static final System.Logger LOGGER = System.getLogger(LoggingLevelTest.class.getName());

    /**
     * The purpose of this test is to verify that you have the logging.properties that shows all log levels.
     * Run with
     *     -Djava.util.logging.config.file=target/test-classes/logging.properties
     * Since pom.xml passes the above JVM parameter to the test runner, you don't need to do this if running from the test from mvn test.
     * You need the parameter when running the test from Eclipse or IntelliJ.
     */
    @Test
    void test() {
        LOGGER.log(Level.ERROR, "Hello Error");
        LOGGER.log(Level.WARNING, "Hello Warning");
        LOGGER.log(Level.INFO, "Hello Info");
        LOGGER.log(Level.DEBUG, "Hello Debug");
        LOGGER.log(Level.TRACE, "Hello Trace");
        
        /* Example standard output
            [SEVERE][18:43:47] myutils.pubsub.LoggingLevelTest test: Hello Error 
            [WARNING][18:43:47] myutils.pubsub.LoggingLevelTest test: Hello Warning 
            [INFO][18:43:47] myutils.pubsub.LoggingLevelTest test: Hello Info 
            [FINE][18:43:47] myutils.pubsub.LoggingLevelTest test: Hello Debug 
            [FINER][18:43:47] myutils.pubsub.LoggingLevelTest test: Hello Trace 
        */
    }

}
