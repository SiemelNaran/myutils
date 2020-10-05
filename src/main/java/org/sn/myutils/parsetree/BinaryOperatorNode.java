package org.sn.myutils.parsetree;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BinaryOperatorNode implements OperatorNode {
    private static final Logger LOGGER = Logger.getLogger(BinaryOperatorNode.class.getName());

    static BinaryOperatorNode tryConstruct(String token,
                                           Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators) throws ConstructException {
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

    /**
     * Tell if this binary operator atomic.
     * 
     * @return true if this node is surrounded by parenthesis, meaning that it cannot be split up for operator precedence rules.
     */
    public final boolean isAtomic() {
        return atomic;
    }

    @Override
    public final boolean isComplete() {
        return left != null && right != null;
    }

    @Override
    public final Class<?> checkEval(Map<String, Class<?>> scopeTypes) throws TypeException {
        Class<?> leftValue = left.checkEval(scopeTypes);
        Class<?> rightValue = right.checkEval(scopeTypes);
        return checkCombine(leftValue, rightValue);
    }

    @Override
    public final Object eval(Map<String, Object> scope) throws EvalException {
        Object leftValue = left.eval(scope);
        Object rightValue = right.eval(scope);
        return combine(leftValue, rightValue);
    }
    
    @Override
    public void reduce(Listener listener) {
        listener.startBinaryOperator(this);
        switch (listener.characteristics().binaryOperatorPosition()) {
            case OPERATOR_FIRST:
                listener.acceptBinaryOperator(this);
                left.reduce(listener);
                listener.nextBinaryOperatorArgument(this);
                right.reduce(listener);
                break;
            case OPERATOR_MIDDLE:
                left.reduce(listener);
                listener.acceptBinaryOperator(this);
                listener.nextBinaryOperatorArgument(this);
                right.reduce(listener);
                break;
            case OPERATOR_LAST:
                left.reduce(listener);
                listener.nextBinaryOperatorArgument(this);
                right.reduce(listener);
                listener.acceptBinaryOperator(this);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        listener.endBinaryOperator(this);
    }
    
    /**
     * Return the precedence of this binary operator.
     * 
     * @return the precedence, higher number means higher precedence (so TIMES would
     *         have a higher number than ADD)
     */
    public abstract int getPrecedence();

    protected abstract Class<?> checkCombine(Class<?> left, Class<?> right) throws TypeException;

    protected abstract Object combine(Object left, Object right) throws EvalException;
}
