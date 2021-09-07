package org.sn.myutils.parsetree;

import java.io.Serial;


public class EvalException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public EvalException(String message) {
        super(message);
    }
}
