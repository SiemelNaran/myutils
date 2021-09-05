package org.sn.myutils.pubsub;


public class CentralServerId extends Number implements Comparable<CentralServerId> {
    private static final long serialVersionUID = 1L;
    
    public static CentralServerId createDefaultFromNow() {
        return new CentralServerId();
    }
    
    private final long value;
    
    private CentralServerId() {
        this(System.currentTimeMillis());
    }
    
    CentralServerId(long value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object thatObject) {
        if (!(thatObject instanceof CentralServerId that)) {
            return false;
        }
        return this.value == that.value;
    }
    
    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }
    
    @Override
    public String toString() {
        return Long.toString(value, 16);
    }

    @Override
    public int intValue() {
        return Long.valueOf(value).intValue();
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public float floatValue() {
        return Long.valueOf(value).floatValue();
    }

    @Override
    public double doubleValue() {
        return Long.valueOf(value).doubleValue();
    }

    @Override
    public int compareTo(CentralServerId that) {
        return Long.compare(this.value, that.value);
    }
}

