package org.sn.myutils.util.parsetree;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class UnaryOperatorNode implements OperatorNode {
    private static final Logger LOGGER = Logger.getLogger(UnaryOperatorNode.class.getName());

    static UnaryOperatorNode tryConstruct(String token,
                                          Map<String, Constructor<? extends UnaryOperatorNode>> map)
            throws ConstructException {
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
    public final boolean isComplete() {
        return node != null;
    }

    @Override
    public Class<?> checkEval(Map<String, Class<?>> scopeTypes) throws TypeException {
        return checkApply(node.checkEval(scopeTypes));
    }

    @Override
    public Object eval(Map<String, Object> scope) throws EvalException {
        return apply(node.eval(scope));
    }

    @Override
    public final void reduce(Listener listener) {
        listener.startUnaryOperator(this);
        switch (listener.characteristics().unaryOperatorPosition()) {
            case OPERATOR_FIRST:
                listener.acceptUnaryOperator(this);
                node.reduce(listener);
                break;
            case OPERATOR_LAST:
                node.reduce(listener);
                listener.acceptUnaryOperator(this);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        listener.endUnaryOperator(this);
    }
    
    protected abstract Class<?> checkApply(Class<?> type) throws TypeException;

    protected abstract Object apply(Object value) throws EvalException;
}
