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

        
        /**
         * Called before visiting a unary operator.
         */
        default void startUnaryOperator(UnaryOperatorNode operator) { }
        
        /**
         * Called to accept the unary operator. Calling code will probably invoke operator.getToken().
         */
        default void acceptUnaryOperator(UnaryOperatorNode operator) { }
        
        /**
         * Called after visiting a unary operator.
         */
        default void endUnaryOperator(UnaryOperatorNode operator) { }
        
        
        /**
         * Called before visiting a binary operator. For example, calling code could add an open parenthesis.
         */
        default void startBinaryOperator(BinaryOperatorNode operator) { }
        
        /**
         * Called to accept the binary operator. Calling code will probably invoke operator.getToken().
         */
        default void acceptBinaryOperator(BinaryOperatorNode operator) { }
        
        /**
         * Called before visiting the right node. For example, calling code could add a comma.
         */
        default void nextBinaryOperatorArgument(BinaryOperatorNode operator) { }
        
        /**
         * Called after visiting a binary operator. For example, you could add a close parenthesis.
         */
        default void endBinaryOperator(BinaryOperatorNode operator) { }
        
        
        /**
         * Called before visiting a function.
         */
        default void startFunction(FunctionNode function) { }
        
        /**
         * Called to accept the function. Calling code will probably invoke function.getName().
         */
        default void acceptFunction(FunctionNode function) { }
        
        /**
         * Called before visiting every argument. For example, calling code could add a comma.
         * 
         * @param function the function node
         * @param argNumber argument number, zero based
         */
        default void nextFunctionArgument(FunctionNode function, int argNumber) { }
        
        /**
         * Called after visiting a function.
         */
        default void endFunction(FunctionNode function) { }


        /**
         * Called before visiting a literal.
         */
        default void startLiteral(LiteralNode literal) { }
        
        /**
         * Called to accept the literal. Calling code will probably invoke literal.toString().
         */
        default void acceptLiteral(LiteralNode literal) { }

        /**
         * Called after visiting a literal.
         */
        default void endLiteral(LiteralNode literal) { }
                

        /**
         * Called before visiting an identifier.
         */
        default void startIdentifier(IdentifierNode identifier) { }

        /**
         * Called to accept the identifier. Calling code will probably invoke identifier.getName().
         */
        default void acceptIdentifier(IdentifierNode identifier) { }

        /**
         * Called after visiting an identifier.
         */
        default void endIdentifier(IdentifierNode identifier) { }
        
        
        public interface Characteristics {
            
            static final Characteristics DEFAULT_CHARACTERISTICS = new Characteristics() {
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
            
            /**
             * Tell the order of traversing a unary operator.
             */
            UnaryOperatorPosition unaryOperatorPosition();
            
            /**
             * Tell the order of traversing a binary operator.
             */
            BinaryOperatorPosition binaryOperatorPosition();
            
            /**
             * Tell the order of traversing a function.
             */
            FunctionPosition functionPosition();
        }
    }
}
