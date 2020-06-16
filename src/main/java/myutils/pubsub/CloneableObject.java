package myutils.pubsub;


public interface CloneableObject<T> extends Cloneable {
    T clone();
}
