package myutils.util.parsetree;

import java.util.Map;
import java.util.Objects;


public class LiteralNode implements ParseNode {
    private final Object value;

    static LiteralNode tryConstruct(String token, NumberFactory numberFactory)
            throws NumberFormatException, ConstructException {
        if (token.length() >= 2 && token.startsWith("\"") && token.endsWith("\"")) {
            return new StringLiteralNodeImpl(token.substring(1, token.length() - 2));
        } else if (token.length() >= 2 && token.startsWith("'") && token.endsWith("'")) {
            return new StringLiteralNodeImpl(token.substring(1, token.length() - 2));
        } else {
            try {
                return new NumberLiteralNodeImpl(numberFactory.fromString(token));
            } catch (NumberFormatException ignored) {
            }
        }
        throw new ConstructException();
    }

    private LiteralNode(Object value) {
        this.value = value;
    }
    
    public Object getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return Objects.toString(value);
    }

    @Override
    public final Class<?> checkEval(Map<String, Class<?>> scopeTypes) {
        return value.getClass();
    }
    
    @Override
    public final Object eval(Map<String, Object> scope) {
        return value;
    }
    
    @Override
    public final void reduce(Listener listener) {
        listener.startLiteral(this);
        listener.acceptLiteral(this);
        listener.endLiteral(this);
    }

    private static final class StringLiteralNodeImpl extends LiteralNode {
        private StringLiteralNodeImpl(String value) {
            super(value);
        }

        @Override
        public String getValue() {
            return (String) super.getValue();
        }
    }

    private static final class NumberLiteralNodeImpl extends LiteralNode {
        private NumberLiteralNodeImpl(Number value) {
            super(value);
        }
        
        @Override
        public Number getValue() {
            return (Number) super.getValue();
        }
    }
}
