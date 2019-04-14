package myutils.util.parsetree;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

import myutils.util.RewindableIterator;
import myutils.util.SimpleStringTokenizerFactory;
import myutils.util.SimpleStringTokenizerFactory.QuoteStrategy;
import myutils.util.SimpleStringTokenizerFactory.Token;


public class ExpressionParser {
    private static final IntPredicate SKIP_CHARACTERS
    = codePoint -> Character.isWhitespace(codePoint);

    private static final List<String> BASIC_SYMBOLS
        = Arrays.asList("(", ")", "[", "]", "{", "}");
    
    private static final IntPredicate LITERAL_CLASS
        = codePoint -> Character.isLetterOrDigit(codePoint) || codePoint == '.';
        
    public static Builder builder() {
        return new Builder();
    }
    
	private final Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators;
    private final Map<String, Constructor<? extends UnaryOperatorNode>> unaryOperators;
    private final Map<String, Constructor<? extends FunctionNode>> functions;
    private final NumberFactory numberFactory;
    private final SimpleStringTokenizerFactory tokenizerFactory;
    
    private ExpressionParser(Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators,
    		                 Map<String, Constructor<? extends UnaryOperatorNode>> unaryOperators,
    		                 Map<String, Constructor<? extends FunctionNode>> functions,
    		                 NumberFactory numberFactory) {
    	this.binaryOperators = binaryOperators;
    	this.unaryOperators = unaryOperators;
    	this.functions = functions;
    	this.numberFactory = numberFactory;
        
    	List<String> symbols = new ArrayList<>(binaryOperators.size() + unaryOperators.size() + BASIC_SYMBOLS.size());
        symbols.addAll(binaryOperators.keySet());
        symbols.addAll(unaryOperators.keySet());
        symbols.addAll(BASIC_SYMBOLS);
        this.tokenizerFactory = new SimpleStringTokenizerFactory(SKIP_CHARACTERS,
                                                                 new QuoteStrategy(true, true),
                                                                 symbols,
                                                                 Collections.singletonList(LITERAL_CLASS));
    }
    
    /**
     * Parse an expression.
     * 
     * @param expression the expression to parse
     * @return root of the parse tree
     * @throws java.text.ParseException if there is an error in the input text, such as unknown operators.
     */
	public ParseNode parse(String expression) throws ParseException {
        Helper helper = new Helper(expression);
        ParseNode tree = helper.innerParse();
        if (helper.parenthesisLevel > 0) {
            throw new ParseException("missing close parenthesis", expression.length());
        }
        return tree;
    }
	
	private class Helper {
	    private final RewindableIterator<Token> tokenizer;
	    private int endOfLastToken;
        private int parenthesisLevel;        
        
        private Helper(String expression) {
            this.tokenizer = RewindableIterator.from(tokenizerFactory.tokenizer(expression));
        }
    
	    private ParseNode innerParse() throws ParseException {
            ParseNode tree = null;
            OperatorNode incomplete = null;
	        while (tokenizer.hasNext()) {
	            Token token = tokenizer.next();
	            endOfLastToken = token.getEnd();
	            if (token.getText().equals(")")) {
	                if (--parenthesisLevel < 0) {
	                    throw new ParseException("too many close parenthesis", token.getStart()); // handles case: )
	                }
                    if (incomplete != null) {
                        throw new ParseException("unexpected close parenthesis", token.getStart()); // handles case: (2 + )
                    }
	                break;
	            } else if (token.getText().equals(",")) {
                    if (incomplete != null) {
                        throw new ParseException("unexpected comma", token.getStart()); // handles case: max(2 + , )
                    }
                    break;
	            } else {
	                final OperatorNode newIncomplete;
	                if (tree == null) {
	                    assert(incomplete == null);
	                    tree = readExpression(token);
	                    newIncomplete = isIncomplete(tree);
	                } else if (incomplete != null) {
                        ParseNode node = readExpression(token);
	                    if (incomplete instanceof UnaryOperatorNode) {
	                        UnaryOperatorNode incompleteAsUnaryOperator = (UnaryOperatorNode) incomplete;
	                        incompleteAsUnaryOperator.setNode(node);
                        } else if (incomplete instanceof BinaryOperatorNode) {
                            BinaryOperatorNode incompleteAsBinaryOperator = (BinaryOperatorNode) incomplete;
                            incompleteAsBinaryOperator.setRight(node);
                        } else {
                            throw new UnsupportedOperationException(incomplete.getClass().getName());
                        }
                        newIncomplete = isIncomplete(node);
                    } else {
                        BinaryOperatorNode nodeAsBinaryOperator = (BinaryOperatorNode) constructNodeFromToken(token, ParseMode.ONLY_BINARY_OPERATORS);
                        if (tree instanceof BinaryOperatorNode
                                && !tree.isAtomic()
                                && ((BinaryOperatorNode) tree).getPrecedence() < nodeAsBinaryOperator.getPrecedence()) {
                            // we just read an operator that has higher precedence
                            // so rearrange the nodes such that the right node of the current tree (say a PLUS node)
                            // becomes the left node of the operator we just read (say a TIMES node)
                            BinaryOperatorNode treeAsBinaryNode = (BinaryOperatorNode) tree;
                            ParseNode oldRight = treeAsBinaryNode.getRight();
                            nodeAsBinaryOperator.setLeft(oldRight);
                            treeAsBinaryNode.setRight(nodeAsBinaryOperator);
                            
                        } else {
                            nodeAsBinaryOperator.setLeft(tree);
                            tree = nodeAsBinaryOperator;
                        }
                        newIncomplete = nodeAsBinaryOperator;
                    }
	                incomplete = newIncomplete;
	            }
	        } // end while
	        
            if (!tokenizer.hasNext() && incomplete != null) {
                throw new ParseException("unexpected end of expression", endOfLastToken - 1); // handles case: 2 +
            }

            return tree;
	    }
	    
	    /**
	     * Read a new expression.  This reads everything besides binary operators.
	     * If `token` is ( then call innerParse to read everything till the closing ) as one parse node.
	     * Otherwise return the literal node, identifier node, or unary operator represented by token.
	     * If the token is an identifier node that is followed by an open parenthesis,
	     * then call innerParse to read the function arguments as well.<p>
	     * 
	     * @param token
	     * @return the ParseNode represented by token
	     * @throws ParseException
	     */
	    private ParseNode readExpression(Token token) throws ParseException {
            if (token.getText().equals("(")) {
                int oldLevel= parenthesisLevel++;
                ParseNode tree = innerParse();
                if (parenthesisLevel > oldLevel) {
                    throw new ParseException("missing close parenthesis", endOfLastToken - 1); // handles case: (2 + 3
                }
                if (tree instanceof BinaryOperatorNode) {
                    ((BinaryOperatorNode) tree).setAtomic();
                }
                return tree;
            } else {
                ParseNode result = constructNodeFromToken(token, ParseMode.EVERYTHING_ELSE);
                if (result instanceof IdentifierNode && tokenizer.hasNext()) {
                    Token nextToken = tokenizer.next();
                    if (nextToken.getText().equals("(")) {
                        IdentifierNode functionName = (IdentifierNode) result;
                        try {
                            result = FunctionNode.tryConstruct(functionName.getIdentifier(), functions);
                            FunctionNode resultAsFunction = (FunctionNode) result;
                            int oldLevel= parenthesisLevel++;
                            while (parenthesisLevel > oldLevel) {
                                ParseNode arg = innerParse();
                                resultAsFunction.add(arg);
                            }
                            if (parenthesisLevel > oldLevel) {
                                throw new ParseException("missing close parenthesis", endOfLastToken - 1); // handles case: max(3, 4
                            }
                            resultAsFunction.checkNumArgs(endOfLastToken - 1);
                        } catch (ConstructException ignored) {
                            throw new ParseException("unrecognized function " + functionName.getIdentifier(), token.getStart());
                        }
                    } else {
                        tokenizer.rewind();
                    }
                }
                return result;
            }
	    }
	}
    
    
    private static OperatorNode isIncomplete(ParseNode node) {
        if (node instanceof OperatorNode) {
            OperatorNode operator = (OperatorNode) node;
            if (!operator.isComplete()) {
                return operator;
            }
        }
        return null;
    }
    
	private enum ParseMode {
	    /**
	     * Only read binary operators
	     */
	    ONLY_BINARY_OPERATORS,
        
	    /**
	     * Read string literals, numbers, identifiers, unary operators.
	     * Does not read functions.
	     */
	    EVERYTHING_ELSE
	}
	
	/**
	 * Parse token into string/number literal, identifier, unary operator, function, or binary operator.
	 * 
	 * @param token the string to parse
	 * @param parseMode what types of nodes to return
	 */
	private ParseNode constructNodeFromToken(Token token, ParseMode parseMode) throws ParseException {
		switch (parseMode) {
		    case ONLY_BINARY_OPERATORS:
	            try {
	                return BinaryOperatorNode.tryConstruct(token.getText(), binaryOperators);
	            } catch (ConstructException ignored) {
	            }
	            break;
            
		    case EVERYTHING_ELSE:
                try {
                    return LiteralNode.tryConstruct(token.getText(), numberFactory);
                } catch (ConstructException ignored) {
                }
                
                try {
                    return IdentifierNode.tryConstruct(token.getText());
                } catch (ConstructException ignored) {
                }

                try {
                    return UnaryOperatorNode.tryConstruct(token.getText(), unaryOperators);
                } catch (ConstructException ignored) {
                }
                break;
		}
		
		throw new ParseException("invalid token " + token.getText(), token.getStart());
	}
	
    public static class InvalidTokenException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        
        private InvalidTokenException(String message) {
            super(message);
        }
    }
    
    public static class Builder {
        private Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators = new HashMap<>();
        private Map<String, Constructor<? extends UnaryOperatorNode>> unaryOperators = new HashMap<>();
        private Map<String, Constructor<? extends FunctionNode>> functions = new HashMap<>();
        private NumberFactory numberFactory = NumberFactory.DEFAULT_NUMBER_FACTORY;
        
        /**
         * Add binary operator.
         * 
         * @param operator the class
         * @return this
         * @throws BuilderException with cause NoSuchMethodException or IllegalAccessException or InstantiationException or InvocationTargetException 
         *         if there is no public default constructor
         * @throws InvalidOperatorException if operator token contains invalid chars
         */
        public Builder addBinaryOperator(Class<? extends BinaryOperatorNode> operator) throws BuilderException {
            try {
                Constructor<? extends BinaryOperatorNode> constructor = operator.getConstructor();
                BinaryOperatorNode instance = constructor.newInstance();
                verifyOperatorValid(instance.getToken());
                binaryOperators.put(instance.getToken(), constructor);
                return this;
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new BuilderException(e);
            }
        }
        
        /**
         * Add unary operator.
         * 
         * @param operator the operator class
         * @return this
         * @throws BuilderException with cause NoSuchMethodException or IllegalAccessException or InstantiationException or InvocationTargetException 
         *         if there is no public default constructor
         * @throws InvalidOperatorException if operator token contains invalid chars, which are: letter/digits, _ " ' ( ) [ ] { } . , ;
         */
        public Builder addUnaryOperator(Class<? extends UnaryOperatorNode> operator) throws BuilderException {
            try {
                Constructor<? extends UnaryOperatorNode> constructor = operator.getConstructor();
                UnaryOperatorNode instance = constructor.newInstance();
                verifyOperatorValid(instance.getToken());
                unaryOperators.put(instance.getToken(), operator.getConstructor());
                return this;
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new BuilderException(e);
            }
        }
        
        /**
         * Add unary operator.
         * 
         * @param operator the operator class
         * @return this
         * @throws BuilderException with cause NoSuchMethodException or IllegalAccessException or InstantiationException or InvocationTargetException 
         *         if there is no public default constructor
         * @throws InvalidOperatorException if function name contains invalid chars, which are: not letter/digits 
         */
        public Builder addFunction(Class<? extends FunctionNode> function) throws BuilderException {
            try {
                Constructor<? extends FunctionNode> constructor = function.getConstructor();
                FunctionNode instance = constructor.newInstance();
                verifyFunctionNameValid(instance.getName());
                functions.put(instance.getName(), constructor);
                return this;
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new BuilderException(e);
            }
        }
        
        private static boolean verifyOperatorValid(String oper) {
            int N = oper.length();
            for (int i = 0; i < N; i++) {
                char c = oper.charAt(i);
                if (Character.isWhitespace(c)
                        || Character.isLetterOrDigit(c)
                        || c == '_'
                        || c == '"'
                        || c == '\''
                        || c == '('
                        || c == ')'
                        || c == '['
                        || c == ']'
                        || c == '{'
                        || c == '}'
                        || c == '.'
                        || c == ','
                        || c == ';') {
                    throw new InvalidOperatorException(Character.toString(c));
                }
            }
            return true;
        }
        
        private static boolean verifyFunctionNameValid(String oper) {
            int N = oper.length();
            for (int i = 0; i < N; i++) {
                char c = oper.charAt(i);
                if (Character.isWhitespace(c)
                        || !Character.isLetterOrDigit(c)) {
                    throw new InvalidOperatorException(Character.toString(c));
                }
            }
            return true;
        }
        
        public Builder setNumberFactory(NumberFactory numberFactory) {
            this.numberFactory = numberFactory;
            return this;
        }
        
        public ExpressionParser build() {
            return new ExpressionParser(binaryOperators,
                                        unaryOperators,
                                        functions,
                                        numberFactory);

        }
        
        public static class BuilderException extends RuntimeException {
            private static final long serialVersionUID = 1L;

            private BuilderException(String message) {
                super(message);
            }
            
            private BuilderException(Exception e) {
                super(e);
            }
        }
        
        public static class InvalidOperatorException extends BuilderException {
            private static final long serialVersionUID = 1L;

            private InvalidOperatorException(String message) {
                super(message);
            }
        }        
    }
}
