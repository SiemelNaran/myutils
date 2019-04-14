package myutils.util.parsetree;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class UnaryOperatorNode implements OperatorNode {
    private static final Logger LOGGER = Logger.getLogger(UnaryOperatorNode.class.getName());
    
    static UnaryOperatorNode tryConstruct(String token, Map<String, Constructor<? extends UnaryOperatorNode>> map) throws ConstructException {
        Constructor<? extends UnaryOperatorNode> constructor = map.get(token);
        if (constructor != null) {
            try {
                return constructor.newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                LOGGER.log(Level.SEVERE, "Unable to construct instance of UnaryOperatorNode", e);
            }
        }
        throw new ConstructException();
    }
    
	private ParseNode node;
	
    protected UnaryOperatorNode() {
    }
    
    void setNode(ParseNode node) {
        this.node = node;
    }
    
    @Override
    public final boolean isAtomic() {
        return true;
    }

    @Override
    public boolean isComplete() {
        return node != null;
    }
    
	@Override
	public Class<?> checkEval(Map<String, Object> scope) throws TypeException {
		return checkApply(node.checkEval(scope));
	}
	
	@Override
	public Object eval(Map<String, Object> scope) throws EvalException {
		return apply(node.eval(scope));
	}
	
	protected abstract Class<?> checkApply(Class<?> type) throws TypeException;
	
	protected abstract Object apply(Object value) throws EvalException;
}
