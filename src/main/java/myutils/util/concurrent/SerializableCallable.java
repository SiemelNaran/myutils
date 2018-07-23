package myutils.util.concurrent;

import java.io.Serializable;
import java.util.concurrent.Callable;


public interface SerializableCallable<T> extends Callable<T>, Serializable {
}
