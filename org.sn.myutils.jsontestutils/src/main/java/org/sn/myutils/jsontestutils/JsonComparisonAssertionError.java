package org.sn.myutils.jsontestutils;

import java.util.List;
import java.util.StringJoiner;


public class JsonComparisonAssertionError extends AssertionError {
    private static final long serialVersionUID = 1;

    private final List<String> errors;

    public JsonComparisonAssertionError(List<String> errors) {
        super("Expected and actual JSON differ");
        this.errors = errors;
    }

    @Override
    public String getMessage() {
        StringJoiner joiner = new StringJoiner("\n", super.getMessage(), "");
        errors.forEach(joiner::add);
        return joiner.toString();
    }

    public List<String> getErrors() {
        return errors;
    }
}
