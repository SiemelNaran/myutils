package org.sn.myutils.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.sn.myutils.testutils.TestUtil.between;

import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.TestBase;


public class PubSubUtilsTest extends TestBase {
    @Test
    void testComputeExponentialBackoff1() {
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 1, 3), equalTo(1000L));
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 2, 3), equalTo(2000L));
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 3, 3), equalTo(4000L));
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 4, 3), equalTo(4000L));
    }

    @Test
    void testComputeExponentialBackoff2() {
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 1, 3, 0.2), between(900L, 1100L));
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 2, 3, 0.2), between(1800L, 2200L));
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 3, 3, 0.2), between(3600L, 4400L));
        assertThat(PubSubUtils.computeExponentialBackoff(1000, 4, 3, 0.2), between(3600L, 4400L));
    }
}

