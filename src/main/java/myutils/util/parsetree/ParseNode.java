package myutils.util.parsetree;

import java.util.Map;


public interface ParseNode {
    /**
     * Check if this parse node is valid.
     * 
     * @param scopeTypes the type of all identifiers in this scope
     * @throws myutils.util.parsetree.TypeException if there is an error. This is a runtime exception.
     */
    Class<?> checkEval(Map<String, Class<?>> scopeTypes) throws myutils.util.parsetree.TypeException;
    
    /**
     * Evaluate this parse node.
     * 
     * @param scope the value of all identifiers in this scope
     * @throws myutils.util.parsetree.EvalException if there is an error. This is a runtime exception.
     */
    Object eval(Map<String, Object> scope) throws myutils.util.parsetree.EvalException;

    /**
     * Visit each node of this parse tree in a depth first manner.
     */
    void reduce(Listener listener);
    
    public interface Listener {
        Characteristics characteristics();

        void startBinaryOperator(BinaryOperatorNode operator);
        void acceptBinaryOperator(BinaryOperatorNode operator);
        void endBinaryOperator(BinaryOperatorNode operator);
        
        void startUnaryOperator(UnaryOperatorNode operator);
        void acceptUnaryOperator(UnaryOperatorNode operator);
        void endUnaryOperator(UnaryOperatorNode operator);
        
        void startFunction(FunctionNode function);
        void acceptFunction(FunctionNode function);
        void endFunction(FunctionNode function);
                
        void startLiteral(LiteralNode literal);
        void acceptLiteral(LiteralNode literal);
        void endLiteral(LiteralNode literal);
                
        void startidentifier(IdentifierNode identifier);
        void acceptIdentifier(IdentifierNode identifier);
        void endIdentifier(IdentifierNode identifier);
    }
    
    public interface Characteristics {
        
        static Characteristics defaultCharacteristics() {
            return new Characteristics() {
                @Override
                public UnaryOperatorPosition unaryOperatorPosition() {
                    return UnaryOperatorPosition.OPERATOR_FIRST;
                }
                
                @Override
                public BinaryOperatorPosition binaryOperatorPosition() {
                    return BinaryOperatorPosition.OPERATOR_MIDDLE;
                }
                
                @Override
                public FunctionPosition functionPosition() {
                    return FunctionPosition.FUNCTION_FIRST;
                }
            };
        }
        
        UnaryOperatorPosition unaryOperatorPosition();
        BinaryOperatorPosition binaryOperatorPosition();
        FunctionPosition functionPosition();
        
        enum UnaryOperatorPosition {
            OPERATOR_FIRST,
            OPERATOR_LAST
        }
        
        enum BinaryOperatorPosition {
            OPERATOR_FIRST,
            OPERATOR_MIDDLE,
            OPERATOR_LAST
        }
        
        enum FunctionPosition {
            FUNCTION_FIRST,
            FUNCTION_LAST
        }
    }
}
