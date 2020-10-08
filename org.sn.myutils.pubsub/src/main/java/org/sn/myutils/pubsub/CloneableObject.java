package org.sn.myutils.pubsub;

import java.io.Serializable;


/**
 * The interface for an object which can be passed between JVMs.
 * It implements Serializable and has a clone function.
 *
 * @param <T> the type of the object which must implement CloneableObject&larr;T&rarr; 
 */
public interface CloneableObject<T> extends Cloneable, Serializable {
    /**
     * Clone this object.
     *
     * <p>Default implementation could be to serialize and de-serialize the object,
     * but we want to force implementors to write a more efficient implementation.
     */
    T clone();

    /**
     * Return an estimate of the number of bytes in this object.
     */
    default int getNumBytes() {
        return 0;
    }
}
