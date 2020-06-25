package myutils.pubsub;

import java.io.Serializable;


public interface CloneableObject<T> extends Cloneable, Serializable {
    T clone();
}
