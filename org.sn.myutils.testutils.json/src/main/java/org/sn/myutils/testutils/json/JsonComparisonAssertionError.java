package org.sn.myutils.testutils.json;

import java.util.List;


public class JsonComparisonAssertionError extends AssertionError {
    private static final long serialVersionUID = 1;

    private final List<String> errors;

    public JsonComparisonAssertionError(List<String> errors) {
        super("Expected and actual JSON differ");
        this.errors = errors;
    }

    public List<String> getErrors() {
        return errors;
    }
}
