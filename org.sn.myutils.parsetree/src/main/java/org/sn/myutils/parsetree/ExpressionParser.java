package org.sn.myutils.parsetree;

import java.io.Serial;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.function.IntPredicate;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.util.RewindableIterator;
import org.sn.myutils.util.SimpleStringTokenizerFactory;
import org.sn.myutils.util.SimpleStringTokenizerFactory.QuoteStrategy;
import org.sn.myutils.util.SimpleStringTokenizerFactory.Token;


public class ExpressionParser {
    private static final IntPredicate SKIP_CHARACTERS = Character::isWhitespace;

    private static final List<String> BASIC_SYMBOLS
        = Arrays.asList("(", ")", "[", "]", "{", "}", ",", ";");
    
    private static final IntPredicate LITERAL_CLASS
        = codePoint -> Character.isLetterOrDigit(codePoint) || codePoint == '.';
        
    public static Builder builder() {
        return new Builder();
    }
    
    private final Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators;
    private final Map<String, Constructor<? extends UnaryOperatorNode>> unaryOperators;
    private final StringCase functionCase;
    private final Map<String, Constructor<? extends FunctionNode>> functions;
    private final NumberFactory numberFactory;
    private final SimpleStringTokenizerFactory tokenizerFactory;

    private ExpressionParser(Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators,
                             Map<String, Constructor<? extends UnaryOperatorNode>> unaryOperators,
                             StringCase functionCase,
                             Map<String, Constructor<? extends FunctionNode>> functions,
                             NumberFactory numberFactory) {
        this.binaryOperators = binaryOperators;
        this.unaryOperators = unaryOperators;
        this.functionCase = functionCase;
        this.functions = functions;
        this.numberFactory = numberFactory;
        
        List<String> symbols = new ArrayList<>(binaryOperators.size() + unaryOperators.size() + BASIC_SYMBOLS.size());
        symbols.addAll(binaryOperators.keySet());
        symbols.addAll(unaryOperators.keySet());
        symbols.addAll(BASIC_SYMBOLS);
        this.tokenizerFactory = new SimpleStringTokenizerFactory(SKIP_CHARACTERS,
                                                                 QuoteStrategy.builder().addSingleQuoteChar().addDoubleQuoteChar().build(),
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
        assert helper.parenthesisLevel == 0;
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
            var binaryStack = new Stack<BinaryOperatorNode>();
            OperatorNode incomplete = null;

            while (tokenizer.hasNext()) {
                Token token = tokenizer.next();
                endOfLastToken = token.getEnd();
                
                if (token.getText().equals(")")) {
                    if (--parenthesisLevel < 0) {
                        throw new ParseException("too many close parenthesis", token.getStart()); // handles case: (2 + 3))
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
                        tree = readExpression(token);
                        // tree can never be a BinaryOperatorNode so no cannot possibly add it to binaryStack
                        newIncomplete = isIncomplete(tree);
                    } else if (incomplete != null) {
                        ParseNode node = readExpression(token);
                        fillIncompleteNode(incomplete, node);
                        newIncomplete = isIncomplete(node);
                    } else {
                        BinaryOperatorNode nodeAsBinaryOperator = (BinaryOperatorNode) constructNodeFromToken(token, ParseMode.ONLY_BINARY_OPERATORS);
                        ParseNode newTree = attachBinaryOperator(tree, binaryStack, nodeAsBinaryOperator);
                        if (newTree != null) {
                            tree = newTree;
                        }
                        newIncomplete = nodeAsBinaryOperator;
                    }
                    
                    incomplete = newIncomplete;
                }
            } // end while
            
            if (!tokenizer.hasNext() && incomplete != null) {
                throw new ParseException("unexpected end of expression", endOfLastToken); // handles case: 2 +
            }

            return tree;
        }
        
        /**
         * Read a new expression.  This reads everything besides binary operators.
         * If `token` is ( then call innerParse to read everything till the closing ) as one parse node.
         * Otherwise, return the literal node, identifier node, or unary operator represented by token.
         * If the token is an identifier node that is followed by an open parenthesis,
         * then call innerParse to read the function arguments as well.
         * 
         * @param token the token to parse, though note that this function may read additional tokens
         * @return the ParseNode represented by token
         * @throws ParseException if there was a parse error
         */
        private ParseNode readExpression(Token token) throws ParseException {
            if (token.getText().equals("(")) {
                int oldLevel = parenthesisLevel++;
                ParseNode tree = innerParse();
                if (parenthesisLevel > oldLevel) {
                    throw new ParseException("missing close parenthesis", endOfLastToken); // handles case: (2 + 3
                }
                if (tree instanceof BinaryOperatorNode binaryOperatorNode) {
                    binaryOperatorNode.setAtomic();
                }
                return tree;
            } else {
                ParseNode result = constructNodeFromToken(token, ParseMode.EVERYTHING_ELSE);
                if (result instanceof IdentifierNode && tokenizer.hasNext()) {
                    Token nextToken = tokenizer.next();
                    if (nextToken.getText().equals("(")) {
                        String functionName = ((IdentifierNode) result).getIdentifier();
                        try {
                            functionName = functionCase.convert(functionName);
                        } catch (IllegalArgumentException ignored) {
                            // function name unchanged, and it won't be found in map
                            // for example if function name is mixed case and functionCase is ALL_LETTERS_SAME_CASE,
                            // so we throw ParseException("unrecognized function ...") below
                        }
                        try {
                            result = FunctionNode.tryConstruct(functionName, functions);
                            FunctionNode resultAsFunction = (FunctionNode) result;
                            int oldLevel = parenthesisLevel++;
                            while (parenthesisLevel > oldLevel) {
                                ParseNode arg = innerParse();
                                if (arg == null) {
                                    break;
                                }
                                resultAsFunction.add(arg);
                            }
                            if (parenthesisLevel > oldLevel) {
                                throw new ParseException("missing close parenthesis in function call", endOfLastToken); // handles case: max(3, 4
                            }
                            resultAsFunction.checkNumArgs(endOfLastToken - 1);
                        } catch (ConstructException ignored) {
                            throw new ParseException("unrecognized function '" + functionName + "'",
                                                     token.getStart()); // handles case: unknown(3, 4)
                        }
                    } else {
                        tokenizer.rewind();
                    }
                }
                return result;
            }
        }

        private void fillIncompleteNode(OperatorNode incomplete, ParseNode node) {
            if (incomplete instanceof UnaryOperatorNode incompleteAsUnaryOperator) {
                incompleteAsUnaryOperator.setNode(node);
            } else if (incomplete instanceof BinaryOperatorNode incompleteAsBinaryOperator) {
                incompleteAsBinaryOperator.setRight(node);
            } else {
                throw new UnsupportedOperationException(incomplete.getClass().getName());
            }
        }

        private ParseNode attachBinaryOperator(ParseNode tree, Stack<BinaryOperatorNode> binaryStack, BinaryOperatorNode nodeAsBinaryOperator) {
            boolean attachedBelow = false;

            for ( ; !binaryStack.isEmpty(); binaryStack.pop()) {
                BinaryOperatorNode parent = binaryStack.peek();
                if (parent.getPrecedence() <= nodeAsBinaryOperator.getPrecedence()) {
                    // we just read an operator that has higher precedence
                    // so rearrange the nodes such that the right node of the current tree (say a PLUS node)
                    // becomes the left node of the operator we just read (say a TIMES node)
                    ParseNode oldRight = parent.getRight();
                    nodeAsBinaryOperator.setLeft(oldRight);
                    parent.setRight(nodeAsBinaryOperator);
                    attachedBelow = true;
                    break;
                }
            }

            ParseNode newTree = null;

            if (!attachedBelow) {
                nodeAsBinaryOperator.setLeft(tree);
                if (binaryStack.isEmpty()) {
                    newTree = nodeAsBinaryOperator;
                } else {
                    BinaryOperatorNode parent = binaryStack.peek();
                    parent.setRight(nodeAsBinaryOperator);
                }
            }

            binaryStack.push(nodeAsBinaryOperator);

            return newTree;
        }
    }
    
    
    private static OperatorNode isIncomplete(ParseNode node) {
        if (node instanceof OperatorNode operator) {
            if (!operator.isComplete()) {
                return operator;
            }
        }
        return null;
    }
    
    private enum ParseMode {
        /**
         * Only read binary operators.
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
            case ONLY_BINARY_OPERATORS -> {
                try {
                    return BinaryOperatorNode.tryConstruct(token.getText(), binaryOperators);
                } catch (ConstructException ignored) {
                }
            }
            case EVERYTHING_ELSE -> {
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
            }
            default -> throw new UnsupportedOperationException();
        }

        throw new ParseException("unrecognized token '" + token.getText() + "'", token.getStart()); // handles case: 2 ?
    }
    
    public static class InvalidTokenException extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;
        
        private InvalidTokenException(String message) {
            super(message);
        }
    }
    
    public static class Builder {
        private final Map<String, Constructor<? extends BinaryOperatorNode>> binaryOperators = new HashMap<>();
        private final Map<String, Constructor<? extends UnaryOperatorNode>> unaryOperators = new HashMap<>();
        private StringCase functionCase = null;
        private final Map<String, Constructor<? extends FunctionNode>> functions = new HashMap<>();
        private NumberFactory numberFactory = DefaultNumberFactory.DEFAULT_NUMBER_FACTORY;

        private Builder() {
        }

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
                verifyPrecedenceValid(instance);
                binaryOperators.put(instance.getToken(), constructor);
                return this;
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new BuilderException(e);
            }
        }

        private void verifyPrecedenceValid(BinaryOperatorNode node) {
            if (node.getPrecedence() <= 0) {
                throw new IllegalArgumentException("Invalid precedence: token=" + node.getToken() + ", precedence=" + node.getPrecedence() + " (should be greater than zero)");
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
         * Set the function case.
         * This function must be called before addFunction.
         * 
         * @param functionCase the function case
         * @return this
         * @throws IllegalStateException if function case has already been set
         */
        public Builder setFunctionCase(@NotNull StringCase functionCase) {
            if (this.functionCase != null) {
                throw new IllegalStateException("functionCase has already been set");
            }
            this.functionCase = Objects.requireNonNull(functionCase);
            return this;
        }
        
        /**
         * Add unary operator.
         * 
         * @param function the function
         * @return this
         * @throws BuilderException with cause NoSuchMethodException or IllegalAccessException or InstantiationException or InvocationTargetException 
         *         if there is no public default constructor
         * @throws InvalidOperatorException if function name contains invalid chars, which are: not letter/digits 
         */
        public Builder addFunction(Class<? extends FunctionNode> function) throws BuilderException {
            Objects.requireNonNull(functionCase);
            try {
                Constructor<? extends FunctionNode> constructor = function.getConstructor();
                FunctionNode instance = constructor.newInstance();
                verifyFunctionNameValid(instance.getName());
                String functionName = functionCase.convert(instance.getName());
                functions.put(functionName, constructor);
                return this;
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new BuilderException(e);
            }
        }

        private static void verifyOperatorValid(String oper) {
            int len = oper.length();
            for (int i = 0; i < len; i++) {
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
                    throw new InvalidOperatorException(oper, c);
                }
            }
        }
        
        private static void verifyFunctionNameValid(String oper) {
            int len = oper.length();
            if (!Character.isLetter(oper.charAt(0))) {
                throw new InvalidOperatorException(oper, oper.charAt(0));                
            }
            for (int i = 0; i < len; i++) {
                char c = oper.charAt(i);
                if (!Character.isLetterOrDigit(c)) {
                    throw new InvalidOperatorException(oper, c);
                }
            }
        }
        
        public Builder setNumberFactory(NumberFactory numberFactory) {
            this.numberFactory = numberFactory;
            return this;
        }
        
        /**
         * Build the parser.
         * 
         * @throws IllegalArgumentException if the substrings of all operators are not operators.
         *                                  For example, if there is a binary operator * and a binary operator ***
         *                                  then this functions throws IllegalArgumentException because ** is not an operator.
         */
        public ExpressionParser build() {
            return new ExpressionParser(binaryOperators,
                                        unaryOperators,
                                        functionCase,
                                        functions,
                                        numberFactory);

        }
        
        public static class BuilderException extends RuntimeException {
            @Serial
            private static final long serialVersionUID = 1L;

            private BuilderException(String message) {
                super(message);
            }
            
            private BuilderException(Exception e) {
                super(e);
            }
        }
        
        public static class InvalidOperatorException extends BuilderException {
            @Serial
            private static final long serialVersionUID = 1L;

            private InvalidOperatorException(String oper, char invalidChar) {
                super("Invalid character " + invalidChar + " in operator " + oper);
            }
        }        
    }
}
