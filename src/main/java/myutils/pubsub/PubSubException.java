package myutils.pubsub;


public class PubSubException extends Exception {
    private static final long serialVersionUID = 1L;

    public PubSubException(String error) {
        super(error);
    }
}
