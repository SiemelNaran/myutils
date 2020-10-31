package org.sn.myutils.pubsub;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;


/**
 * This class is used for sharding.
 * It provides functionality to maps a key to a remote machine.
 * The key is the topic.
 * 
 * <p>Since each remote machine is used in a socket,
 * each remote machine is mapped to a specific port on the local machine,
 * so there is functionality to map a remote machine to a port on the local machine.
 */
public abstract class KeyToSocketAddressMapper {
    
    private final Map<SocketAddress, SocketAddress> remoteToLocalMap = new ConcurrentHashMap<>();
    
    /**
     * Map a key to remote machine.
     *
     * @param key the key (i.e. the topic)
     * @return address of the remote machine
     * @throws IllegalStateException if the remote machine returned by the abstract protected function is not in the universe of possible machines 
     */
    public final @Nonnull SocketAddress mapKeyToRemoteAddress(String key) {
        var result = Objects.requireNonNull(doMapKeyToRemoteAddress(key));
        var universe = getRemoteUniverse();
        if (!universe.contains(result)) {
            throw new IllegalStateException("Invalid mapping: key not in universe: key=" + key + ", universe=" + universe + ", result=" + result);
        }
        return result;
    }

    /**
     * Get the local address for the given remote machine.
     * This function looks up in the local map, and if not found calls the protected abstract generator function.
     * 
     * @param remoteAddress the remote address
     * @return the local address. This will usually be localhost and a unique port.
     */
    public final @Nonnull SocketAddress getLocalAddress(SocketAddress remoteAddress) {
        return remoteToLocalMap.computeIfAbsent(remoteAddress, unused -> generateLocalAddress());
    }
    
    /**
     * Return a collection of all possible remote machines.
     */
    public abstract Set<SocketAddress> getRemoteUniverse();
    
    /**
     * Map a key to a remote machine in the universe.
     */
    protected abstract @Nonnull SocketAddress doMapKeyToRemoteAddress(String key);
    
    /**
     * Create a new local address.
     * Should throw if user is requesting more socket local ports than are in the universe.
     */
    protected abstract @Nonnull SocketAddress generateLocalAddress();

    /**
     * Return a KeyToSocketAddressMapper for a single remote machine and local machine port.
     */
    public static KeyToSocketAddressMapper forSingleHostAndPort(String localHost, int localPort, String remoteHost, int remotePort) {
        var localAddress = new AtomicReference<SocketAddress>(new InetSocketAddress(localHost, localPort));
        var remoteAddress = new InetSocketAddress(remoteHost, remotePort);
        
        return new KeyToSocketAddressMapper() {
            @Override
            public Set<SocketAddress> getRemoteUniverse() {
                return Set.of(remoteAddress);
            }

            @Override
            protected @Nonnull SocketAddress doMapKeyToRemoteAddress(String key) {
                return remoteAddress;
            }

            @Override
            protected @Nonnull SocketAddress generateLocalAddress() {
                // the purpose of the Objects.requireNonNull and getAndSet is to ensure that the 2nd call to this function throws an exception
                return Objects.requireNonNull(localAddress.getAndSet(null));
            }
        };
    }
}
