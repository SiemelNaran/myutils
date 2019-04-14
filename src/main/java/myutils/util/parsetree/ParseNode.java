package myutils.util.parsetree;

import java.util.Map;


public interface ParseNode {
    boolean isAtomic();
    Class<?> checkEval(Map<String, Object> scope) throws TypeException;
    Object eval(Map<String, Object> scope) throws EvalException;
}
