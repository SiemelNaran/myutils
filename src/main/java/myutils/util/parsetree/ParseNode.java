package myutils.util.parsetree;

import java.util.Map;


public interface ParseNode {
    /**
     * Check if this parse node is valid.
     * 
     * @param scopeTypes the type of all identifiers in this scope
     * @throws myutils.util.parsetree.TypeException if there is an error
     */
    Class<?> checkEval(Map<String, Class<?>> scopeTypes) throws TypeException;
    
    /**
     * Evaluate this parse node.
     * 
     * @param scope the value of all identifiers in this scope
     * @throws myutils.util.parsetree.EvalException if there is an error
     */
    Object eval(Map<String, Object> scope) throws EvalException;
}
