package org.sn.myutils;

import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;


/**
 * Base test class that logs when tests are started and end, and when each test is started and ended.
 * This makes it easier to study console logs of running all tests.
 * Eclipse does not show the stdout/stderr for each test separately (but in IntelliJ when you click a test you see the stdout/stderr for that test).
 * 
 * <p>There is also a function getStartOfTime.
 */
@ExtendWith(LogFailureToConsoleTestWatcher.class)
public abstract class TestBase {
    private Instant startOfTime;

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
        startOfTime = Instant.now();
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("test started: " + testInfo.getDisplayName());
    }
    
    @AfterEach
    void printTestFinished(TestInfo testInfo) {
        System.out.println("test finished: " + testInfo.getDisplayName());
    }
    
    protected final Instant getStartOfTime() {
        return startOfTime;
    }
}
