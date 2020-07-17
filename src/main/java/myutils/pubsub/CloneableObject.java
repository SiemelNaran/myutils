package myutils.pubsub;

import java.io.Serializable;


/**
 * The interface for an object which can be passed between JVMs.
 * It implements Serializable and has a clone function.
 *
 * @param <T> the type of the object which must implement CloneableObject<T> 
 */
public interface CloneableObject<T> extends Cloneable, Serializable {
    /**
     * Clone this object.
     */
    T clone();

    /**
     * Return an estimate of the number of bytes in this object.
     */
    default int getNumBytes() {
        return 0;
    }
}
