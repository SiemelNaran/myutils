package org.sn.myutils.pubsub;

import java.io.Serial;


public class PubSubException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public PubSubException(String error) {
        super(error);
    }
}
