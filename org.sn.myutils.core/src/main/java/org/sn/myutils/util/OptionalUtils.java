package org.sn.myutils.util;

import java.util.Optional;


public class OptionalUtils {
    private OptionalUtils() {
    }
    
    @SuppressWarnings("unchecked")
    public static <T> Optional<T> of(Object val, Class<T> clazz) {
        if (clazz.isInstance(val)) {
            return Optional.of((T) val);
        } else {
            return Optional.empty();
        }
    }
}
