package myutils.util.parsetree;


public interface OperatorNode extends ParseNode {
    /**
     * Return the operator of this token.
     * 
     * @return the string describing this operator such as * or **
     */
    String getToken();
    
    /**
     * Used during parsing to determine if this operator node has all its child nodes filled in.
     * After parsing, isComplete will be true for each operator node in the parse tree.
     */
    boolean isComplete();
}
