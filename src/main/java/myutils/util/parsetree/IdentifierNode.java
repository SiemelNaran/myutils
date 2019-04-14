package myutils.util.parsetree;

import java.util.Map;

public class IdentifierNode implements ParseNode {
    private final String identifier;

    static IdentifierNode tryConstruct(String token) throws ConstructException {
        if (!isIdentifier(token)) {
            throw new ConstructException();
        }
        return new IdentifierNode(token);
    }
    
    private static boolean isIdentifier(String token) {
        final int N = token.length();
        if (N == 0 || !isValidStartChar(token.charAt(0))) {
            return false;
        }
        for (int i = 1; i < N; i++) {
            char c = token.charAt(i);
            if (!isValidChar(c)) {
                return false;
            }
        }
        return true;
    }
    
    private static boolean isValidStartChar(char c) {
        return Character.isLetter(c) || c == '_'; 
    }

    private static boolean isValidChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    private IdentifierNode(String identifier) {
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }
    
    @Override
    public final boolean isAtomic() {
        return true;
    }

    @Override
    public Class<?> checkEval(Map<String, Object> scope) {
        Object result = scope.get(identifier);
        if (result == null) {
            throw new VariableNotFoundTypeException(identifier);
        }
        return result.getClass();
    }

    @Override
    public Object eval(Map<String, Object> scope) {
        Object result = scope.get(identifier);
        if (result == null) {
            throw new VariableNotFoundEvalException(identifier);
        }
        return result;
    }
    
    public static class VariableNotFoundTypeException extends EvalException {
        private static final long serialVersionUID = 1L;

        VariableNotFoundTypeException(String identifier) {
            super(identifier + " not found");
        }
    }
    
    public static class VariableNotFoundEvalException extends EvalException {
        private static final long serialVersionUID = 1L;

        VariableNotFoundEvalException(String identifier) {
            super(identifier + " not found");
        }
    }
}
