package myutils.util.parsetree;

import java.util.Map;

public class LiteralNode implements ParseNode {
	private final Object value;
	
	static LiteralNode tryConstruct(String token, NumberFactory numberFactory) throws NumberFormatException, ConstructException {
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
	
	@Override
	public Class<?> checkEval(Map<String, Class<?>> scopeTypes) {
		return value.getClass();
	}
	
	@Override
	public Object eval(Map<String, Object> scope) {
		return value;
	}
    
    private static class StringLiteralNodeImpl extends LiteralNode {
        private StringLiteralNodeImpl(String value) {
            super(value);
        }
    }
    
    private static class NumberLiteralNodeImpl extends LiteralNode {
        private NumberLiteralNodeImpl(Number value) {
            super(value);
        }
    }
}
