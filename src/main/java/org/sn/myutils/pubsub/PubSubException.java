package org.sn.myutils.pubsub;

import org.sn.myutils.pubsub.MessageClasses.InvalidMessage;


public class PubSubException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PubSubException(String error) {
        super(error);
    }
    
    InvalidMessage toInvalidMessage() {
        return new InvalidMessage(getMessage());
    }
}
