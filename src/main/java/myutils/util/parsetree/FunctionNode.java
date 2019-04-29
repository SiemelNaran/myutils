package myutils.util.parsetree;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public abstract class FunctionNode implements ParseNode {
    private static final Logger LOGGER = Logger.getLogger(FunctionNode.class.getName());
    
    static FunctionNode tryConstruct(String token, Map<String, Constructor<? extends FunctionNode>> map) throws ConstructException {
        Constructor<? extends FunctionNode> constructor = map.get(token);
        if (constructor != null) {
            try {
                return constructor.newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                LOGGER.log(Level.SEVERE, "Unable to construct instance of FunctionNode", e);
            }
        }
        throw new ConstructException();
    }

    private final List<ParseNode> args = new ArrayList<>();
	
	protected FunctionNode() {
	}
	
	void add(ParseNode child) {
	    args.add(child);
	}
	
	void checkNumArgs(int tokenStart) throws ParseException {
        if (args.size() < getMinArgs()) {
            throw new ParseException("too few arguments to function " + getName(), tokenStart);
        }
        if (args.size() > getMaxArgs()) {
            throw new ParseException("too many arguments to function " + getName(), tokenStart);
        }
	}
	
	@Override
	public Class<?> checkEval(Map<String, Class<?>> scopeTypes) throws TypeException {
		return checkCombine(args.stream().map(node -> node.checkEval(scopeTypes)).collect(Collectors.toList()));
	}
	
	@Override
	public Object eval(Map<String, Object> scope) throws EvalException {
		return combine(args.stream().map(node -> node.eval(scope)).collect(Collectors.toList()));
	}
	
    protected abstract String getName();
    
    /**
     * @return The minimum number of arguments. Return 0 if there is no minimum.
     */
    protected abstract int getMinArgs();
    
    /**
     * @return The maximum number of arguments. Return Integer.MAX_VALUE if there is no maximum.
     */
    protected abstract int getMaxArgs();
    
	protected abstract Class<?> checkCombine(List<Class<?>> args) throws TypeException;
	
	protected abstract Object combine(List<Object> args) throws EvalException;
}