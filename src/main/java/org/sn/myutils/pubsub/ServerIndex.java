package org.sn.myutils.pubsub;

import java.math.BigInteger;


/**
 * A class representing a server index.
 * This is a 128 bit integer.
 * The first 64 bits are the epoch milliseconds of when the server was started.
 * The next 64 bits are a monotonically increasing long.
 */
public class ServerIndex extends Number implements Comparable<ServerIndex> {
    private static final long serialVersionUID = 1L;
    
    public static final ServerIndex MIN_VALUE = new ServerIndex(BigInteger.ZERO);
    public static final ServerIndex MAX_VALUE = new ServerIndex(BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(Long.SIZE).or(new BigInteger("FFFFFFFFFFFFFFFF", 16)));
    
    private final BigInteger value;
    
    ServerIndex(CentralServerId centralServerId) {
        value = BigInteger.valueOf(centralServerId.longValue()).shiftLeft(Long.SIZE);
    }
    
    private ServerIndex(BigInteger value) {
        this.value = value;
    }
    
    public ServerIndex increment() {
        return new ServerIndex(value.add(BigInteger.ONE));
    }

    @Override
    public boolean equals(Object thatObject) {
        if (!(thatObject instanceof ServerIndex)) {
            return false;
        }
        ServerIndex that = (ServerIndex) thatObject;
        return this.value.equals(that.value);
    }
    
    @Override
    public int hashCode() {
        return value.hashCode();
    }
    
    @Override
    public String toString() {
        return value.toString(16);
    }

    @Override
    public int intValue() {
        return value.intValue();
    }

    @Override
    public long longValue() {
        return value.longValue();
    }

    @Override
    public float floatValue() {
        return value.floatValue();
    }

    @Override
    public double doubleValue() {
        return value.doubleValue();
    }

    @Override
    public int compareTo(ServerIndex that) {
        return this.value.compareTo(that.value);
    }

    public static int compare(ServerIndex lhs, ServerIndex rhs) {
        return lhs.value.compareTo(rhs.value);
    }
    
    public CentralServerId extractCentralServerId() {
        return new CentralServerId(value.shiftRight(Long.SIZE).longValue());
    }
}
