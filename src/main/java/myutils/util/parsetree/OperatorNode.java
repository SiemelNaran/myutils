package myutils.util.parsetree;


public interface OperatorNode extends ParseNode {
    /**
     * The string describing this operator.
     */
    String getToken();
    
    /**
     * Used during parsing to determine if this operator node has all its child nodes filled in.
     * After parsing, isComplete should be true for each operator node in the parse tree.
     */
    boolean isComplete();
}
