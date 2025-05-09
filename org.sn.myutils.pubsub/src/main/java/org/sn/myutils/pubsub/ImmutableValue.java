package org.sn.myutils.pubsub;

import java.util.Objects;


class ImmutableValue<T> {
    private T value;

    public ImmutableValue() {
    }

    public void set(T value) {
        if (value == null) {
            throw new IllegalStateException("Value already set");
        }
        this.value = value;
    }

    public T get() {
        return Objects.requireNonNull(value);
    }
}
