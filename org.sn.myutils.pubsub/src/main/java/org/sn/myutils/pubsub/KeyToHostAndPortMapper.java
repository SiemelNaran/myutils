package org.sn.myutils.pubsub;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;


public abstract class KeyToHostAndPortMapper {
    
    private final Map<SocketAddress, SocketAddress> remoteToLocalMap = new HashMap<>();
    
    public @Nonnull SocketAddress mapKeyToRemoteAddress(String key) {
        var result = Objects.requireNonNull(doMapKeyToRemoteAddress(key));
        var universe = getRemoteUniverse();
        if (!universe.contains(result)) {
            throw new IllegalStateException("Invalid mapping: key=" + key + ", universe=" + universe + ", result=" + result);
        }
        return result;
    }
    
    public @Nonnull SocketAddress getLocalAddress(SocketAddress remoteAddress) {
        var localAddress = remoteToLocalMap.get(remoteAddress);
        if (localAddress == null) {
            synchronized (remoteToLocalMap) {
                localAddress = remoteToLocalMap.get(remoteAddress);
                if (localAddress == null) {
                    localAddress = generateLocalAddress();
                    remoteToLocalMap.put(remoteAddress, localAddress);
                }
            }
        }
        return localAddress;
    }
    
    public abstract Collection<SocketAddress> getRemoteUniverse();
    
    protected abstract @Nonnull SocketAddress doMapKeyToRemoteAddress(String key);
    
    protected abstract @Nonnull SocketAddress generateLocalAddress();
    
    public static KeyToHostAndPortMapper forSingleHostAndPort(String localHost, int localPort, String remoteHost, int remotePort) {
        var localAddress = new AtomicReference<SocketAddress>(InetSocketAddress.createUnresolved(localHost, localPort));
        var remoteAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort);
        
        return new KeyToHostAndPortMapper() {
            @Override
            public Collection<SocketAddress> getRemoteUniverse() {
                return Collections.singletonList(remoteAddress);
            }

            @Override
            protected @Nonnull SocketAddress doMapKeyToRemoteAddress(String key) {
                return remoteAddress;
            }

            @Override
            protected @Nonnull SocketAddress generateLocalAddress() {
                return Objects.requireNonNull(localAddress.getAndSet(null));
            }
        };
    }
}
