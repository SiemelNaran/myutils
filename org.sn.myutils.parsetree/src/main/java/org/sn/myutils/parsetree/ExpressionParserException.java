package org.sn.myutils.parsetree;

import java.io.Serial;

public class ExpressionParserException extends Exception {
    @Serial
    private static final long serialVersionUID = 1L;

    ExpressionParserException(String message) {
        super(message);
    }
}
