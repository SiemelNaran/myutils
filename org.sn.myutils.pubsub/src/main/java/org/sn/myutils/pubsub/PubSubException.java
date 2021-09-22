package org.sn.myutils.pubsub;

import java.io.Serial;
import org.sn.myutils.pubsub.MessageClasses.InvalidMessage;


public class PubSubException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public PubSubException(String error) {
        super(error);
    }
    
    InvalidMessage toInvalidMessage() {
        return new InvalidMessage(getMessage());
    }
}
