package myutils.util.parsetree;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class DefaultNumberFactory implements NumberFactory {

    public enum IntegerPolicy {
        PREFER_INTEGER,
        PREFER_LONG,
        PREFER_BIG_INTEGER
    };
    
    public enum FloatPolicy {
        PREFER_FLOAT,
        PREFER_DOUBLE,
        PREFER_BIG_DECIMAL
    };
    
    static final NumberFactory DEFAULT_NUMBER_FACTORY = DefaultNumberFactory.builder().build();
    
    public static Builder builder() {
        return new Builder();
    }
    
    private final @Nullable IntegerPolicy integerPolicy;
    private final @Nonnull FloatPolicy floatPolicy;
    private final @Nullable Integer bigDecimalScale;
    
    private DefaultNumberFactory(IntegerPolicy integerPolicy,
                                 FloatPolicy floatPolicy,
                                 Integer bigDecimalScale) {
        this.integerPolicy = integerPolicy;
        this.floatPolicy = floatPolicy;
        this.bigDecimalScale = bigDecimalScale;
    }

    @Override
    public Number fromString(String str) throws NumberFormatException {
        if (str == null) {
            return null;
        }
        if (integerPolicy != null && isInteger(str)) {
            return constructInteger(str);
        }
        if (isFloat(str)) {
            return constructFloat(str);
        }
        throw new NumberFormatException("unable to determine type of number");
    }
    
    private boolean isInteger(String token) {
        final int N = token.length();
        char firstChar = token.charAt(0);
        int firstDigitIndex = firstChar == '+' || firstChar == '-' ? 1 : 0;
        for (int i = firstDigitIndex; i < N; i++) {
            char c = token.charAt(i);
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    private Number constructInteger(String str) {
        switch (integerPolicy) {
            case PREFER_INTEGER:
                try {
                    return Integer.parseInt(str);
                } catch (NumberFormatException ignored) {
                }
                
            case PREFER_LONG:
                try {
                    return Long.parseLong(str);
                } catch (NumberFormatException ignored) {
                }
                
            default:
                return new BigInteger(str);
        }
    }
    
    private boolean isFloat(String token) {
        return true;
    }

    private Number constructFloat(String str) {
        switch (floatPolicy) {
            case PREFER_FLOAT:
                try {
                    return Float.parseFloat(str);
                } catch (NumberFormatException ignored) {
                }
                
            case PREFER_DOUBLE:
                try {
                    return Double.parseDouble(str);
                } catch (NumberFormatException ignored) {
                }
                
            default:
                BigDecimal number = new BigDecimal(str);
                if (bigDecimalScale != null) {
                    number = number.setScale(bigDecimalScale);
                }
                return number;
        }
    }

    public static class Builder {
        private @Nullable IntegerPolicy integerPolicy = IntegerPolicy.PREFER_INTEGER;
        private @Nonnull FloatPolicy floatPolicy = FloatPolicy.PREFER_DOUBLE;
        private @Nullable Integer bigDecimalScale;
        
        /**
         * @param integerPolicy the way to handle integers, pass in null if all numbers should be floats
         */
        public Builder setIntegerPolicy(@Nullable IntegerPolicy integerPolicy) {
            this.integerPolicy = integerPolicy;
            return this;
        }
        
        public Builder setFloatPolicy(@Nonnull FloatPolicy floatPolicy) {
            this.floatPolicy = Objects.requireNonNull(floatPolicy);
            return this;
        }
        
        public Builder setBigDecimalScale(@Nullable Integer bigDecimalScale) {
            this.bigDecimalScale = bigDecimalScale;
            return this;
        }
        
        public DefaultNumberFactory build() {
            return new DefaultNumberFactory(integerPolicy, floatPolicy, bigDecimalScale);
        }
    }
}