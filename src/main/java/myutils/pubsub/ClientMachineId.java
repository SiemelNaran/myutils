package myutils.pubsub;

import java.lang.constant.Constable;
import java.lang.constant.ConstantDesc;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;


public final class ClientMachineId implements java.io.Serializable, Comparable<ClientMachineId>, Constable, ConstantDesc, CharSequence {
    private static final long serialVersionUID = 1L;
    
    private final @Nonnull String value;
    
    public ClientMachineId(@Nonnull String value) {
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
    public String toString() {
        return value.toString();
    }

    @Override
    public int compareTo(ClientMachineId that) {
        return this.value.compareTo(that.value);
    }
    
    @Override
    public Optional<? extends ConstantDesc> describeConstable() {
        return value.describeConstable();
    }

    @Override
    public Object resolveConstantDesc(Lookup lookup) throws ReflectiveOperationException {
        return value.resolveConstantDesc(lookup);
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
