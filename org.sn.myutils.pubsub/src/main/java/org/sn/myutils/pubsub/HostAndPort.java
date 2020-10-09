package org.sn.myutils.pubsub;

import java.lang.constant.Constable;
import java.lang.constant.ConstantDesc;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Objects;
import java.util.Optional;


public final class HostAndPort implements java.io.Serializable, Constable, ConstantDesc {
    private static final long serialVersionUID = 1L;
    
    private final String host;
    private final int port;
    
    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    @Override
    public boolean equals(Object thatObject) {
        if (!(thatObject instanceof HostAndPort)) {
            return false;
        }
        
        HostAndPort that = (HostAndPort) thatObject;
        return this.host.equals(that.host) && this.port == that.port;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }
    
    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public Optional<HostAndPort> describeConstable() {
        return Optional.of(this);
    }

    @Override
    public Object resolveConstantDesc(Lookup lookup) {
        return this;
    }

    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
}
