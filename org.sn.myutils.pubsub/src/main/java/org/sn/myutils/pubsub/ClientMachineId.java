package org.sn.myutils.pubsub;

import java.lang.constant.Constable;
import java.lang.constant.ConstantDesc;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Objects;
import java.util.Optional;
import org.sn.myutils.annotations.NotNull;


public final class ClientMachineId implements java.io.Serializable, Comparable<ClientMachineId>, CharSequence, Constable, ConstantDesc {
    private static final long serialVersionUID = 1L;
    
    private final @NotNull String value;
    
    public ClientMachineId(@NotNull String value) {
        this.value = Objects.requireNonNull(value);
    }
    
    @Override
    public boolean equals(Object thatObject) {
        if (!(thatObject instanceof ClientMachineId)) {
            return false;
        }
        
        ClientMachineId that = (ClientMachineId) thatObject;
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
    
    @Override
    public Optional<ClientMachineId> describeConstable() {
        return Optional.of(this);
    }

    @Override
    public ClientMachineId resolveConstantDesc(Lookup lookup) {
        return this;
    }
}
