package myutils.util.parsetree;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class BinaryOperatorNode implements OperatorNode {
    private static final Logger LOGGER = Logger.getLogger(BinaryOperatorNode.class.getName());
    
    static BinaryOperatorNode tryConstruct(CharSequence token, Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators) throws ConstructException {
        Constructor<? extends BinaryOperatorNode> constructor = binaryOperators.get(token);
        if (constructor != null) {
            try {
                return constructor.newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                LOGGER.log(Level.SEVERE, "Unable to construct instance of BinaryOperatorNode", e);
            }
        }
        throw new ConstructException();
    }

    private ParseNode left;
	private ParseNode right;
    private boolean atomic;
	
	protected BinaryOperatorNode() {
	}
	
    void setLeft(ParseNode left) {
        this.left = left;
    }
    
    void setRight(ParseNode right) {
        this.right = right;
    }
    
    ParseNode getRight() {
        return right;
    }
    
    void setAtomic() {
        atomic = true;
    }
    
    @Override
    public boolean isAtomic() {
        return atomic;
    }

    @Override
    public boolean isComplete() {
        return left != null && right != null;
    }
	
    @Override
	public Class<?> checkEval(Map<String, Object> scope) throws TypeException {
		Class<?> leftValue = left.checkEval(scope);
		Class<?> rightValue = right.checkEval(scope);
		return checkCombine(leftValue, rightValue);
	}
	
	@Override
	public Object eval(Map<String, Object> scope) throws EvalException {
		Object leftValue = left.eval(scope);
		Object rightValue = right.eval(scope);
		return combine(leftValue, rightValue);
	}
	

    /**
     * @return the precedence, higher number means higher precedence (so TIMES would have a higher nunber than ADD)
     */
	public abstract int getPrecedence();
	
    protected abstract Class<?> checkCombine(Class<?> left, Class<?> right) throws TypeException;
	
	protected abstract Object combine(Object left, Object right) throws EvalException;
}
