package org.sn.myutils.pubsub;

import java.io.Serial;
import java.util.Objects;
import org.sn.myutils.annotations.NotNull;


public final class ClientMachineId implements java.io.Serializable, Comparable<ClientMachineId>, CharSequence {
    @Serial
    private static final long serialVersionUID = 1L;
    
    private final @NotNull String value;
    
    public ClientMachineId(@NotNull String value) {
        this.value = Objects.requireNonNull(value);
    }
    
    @Override
    public boolean equals(Object thatObject) {
        if (!(thatObject instanceof ClientMachineId that)) {
            return false;
        }

        return this.value.equals(that.value);
    }
    
    @Override
    public int hashCode() {
        return value.hashCode();
    }
    
    @Override
    public @NotNull String toString() {
        return value;
    }

    @Override
    public int compareTo(ClientMachineId that) {
        return this.value.compareTo(that.value);
    }

    @Override
    public int length() {
        return value.length();
    }

    @Override
    public char charAt(int index) {
        return value.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return value.subSequence(start, end);
    }
}
