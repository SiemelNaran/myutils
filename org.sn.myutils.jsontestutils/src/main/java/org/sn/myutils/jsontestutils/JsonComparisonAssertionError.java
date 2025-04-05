package org.sn.myutils.jsontestutils;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.StringJoiner;


public class JsonComparisonAssertionError extends AssertionError {
    @Serial
    private static final long serialVersionUID = 1;

    private final Serializable errors;

    public JsonComparisonAssertionError(List<String> errors) {
        super("Expected and actual JSON differ");
        this.errors = (Serializable) errors;
    }

    @Override
    public String getMessage() {
        StringJoiner joiner = new StringJoiner("\n", super.getMessage(), "");
        getErrors().forEach(joiner::add);
        return joiner.toString();
    }

    @SuppressWarnings("unchecked")
    public List<String> getErrors() {
        return (List<String>) errors;
    }
}
