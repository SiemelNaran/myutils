package org.sn.myutils.testutils.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;


public class JsonComparisonAssertionErrorTest {
    @Test
    public void test() {
        var errors = List.of("[recordId=r1].firstName=First: Expected First but got FirstWrong",
                             "[recordId=r1].middleName=ExtraAttribute: Attribute in actual but not in expected");
        JsonComparisonAssertionError error = new JsonComparisonAssertionError(errors);
        assertSame(errors, error.getErrors());
        String message = error.getMessage();
        assertThat(message, Matchers.startsWith("Expected and actual JSON differ"));
        assertThat(message, Matchers.stringContainsInOrder("Expected and actual JSON differ",
                                                           "[recordId=r1].firstName=First: Expected First but got FirstWrong",
                                                           "[recordId=r1].middleName=ExtraAttribute: Attribute in actual but not in expected"));
    }
}
