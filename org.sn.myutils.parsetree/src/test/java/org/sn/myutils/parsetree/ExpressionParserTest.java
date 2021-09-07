package org.sn.myutils.parsetree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.sn.myutils.testutils.TestUtil.assertExceptionFromCallable;

import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.sn.myutils.parsetree.ParseNode.Listener;
import org.sn.myutils.parsetree.UnitNumberFactory.UnitPosition;


@SuppressWarnings("OptionalGetWithoutIsPresent")
public class ExpressionParserTest {
    @Test
    void testEvaluate() throws ParseException {
        Map<String, Object> scope = Collections.emptyMap();
        
        assertEquals(14, evaluate("2+3*4", scope));
        assertEquals(10, evaluate("2*3+4", scope));
        assertEquals(20, evaluate("(2+3)*4", scope));
        assertEquals(-10, evaluate("2+-3*4", scope));
        assertEquals(14, evaluate("2+--3*4", scope));
        assertEquals(10, evaluate("-2+3*4", scope));
        assertEquals(14, evaluate("--2+3*4", scope));
        assertEquals(-14, evaluate("-(2+3*4)", scope));
        assertEquals(14, evaluate("--(2+3*4)", scope));

        assertEquals(10, evaluate("2*3+4", scope));
        assertEquals(14, evaluate("2*(3+4)", scope));
        assertEquals(28, evaluate("2*(3+4)*(1+1)", scope));

        assertEquals(14, evaluate("((2+3*4))", scope));
        assertEquals(14, evaluate("+((2+3*4))", scope));
        assertEquals(14, evaluate("++--(--(2+3*4))", scope));
        
        assertEquals(67, evaluate("2+3*4*5+10/2", scope)); // 2 + 60 + 5
    }
    
    @Test
    void testEvaluateSymbols() throws ParseException {
        Map<String, Object> scope = new HashMap<>();
        scope.put("x", 3);
        scope.put("y", 4);
        assertEquals(14, evaluate("2+x*y", scope));
    }
    
    @Test
    void testEvaluateFunction() throws ParseException {
        Map<String, Object> scope = new HashMap<>();
        scope.put("x", 3);
        scope.put("y", 4);
        assertEquals(14, evaluate("2+x*max(---5,y)", scope)); // 2 + 3 * 4
        assertEquals(14, evaluate("2+x*MAX(---5,y)", scope)); // 2 + 3 * 4
        assertEquals(0, evaluate("3 + (min(5, 1) + -2) * 3", scope)); // 3 + (1 + -2) * 3 = 3 * -1 * 3 = 0
    }
    
    @Test
    void testInvalidExpressions() {
        assertParseError("(2 + 3))", "too many close parenthesis", 7); // index of last )
        assertParseError("(2 + )", "unexpected close parenthesis", 5); // index of )
        assertParseError("max(2 + , )", "unexpected comma", 8); // index of comma
        assertParseError("2 +", "unexpected end of expression", 3); // one past end of expression
        assertParseError("(2 + 3", "missing close parenthesis", 6); // one past end of expression
        assertParseError("max(3, 4", "missing close parenthesis in function call", 8); // one past end of of expression
        assertParseError("unknown(3, 4)", "unrecognized function 'unknown'", 0); // index of start of expression
        assertParseError("Max(3, 4)", "unrecognized function 'Max'", 0); // index of start of expression
        assertParseError("2 * * 3", "unrecognized token '*'", 4); // index of second *
        assertParseError("2 ^ 3", "unrecognized token '^'", 2); // index of ^
    }
    
    @Test
    void testNoExpression() throws ParseException {
        ParseNode tree = PARSER.parse("    ");
        assertNull(tree);
    }
    
    @Test
    void testEvaluateUnits() throws ParseException {
        Map<String, Object> scope = new HashMap<>();
        scope.put("x", 1_000_000);
        assertEquals(Integer.valueOf(1_003_002), evaluate("2m+3km+x", scope));
    }
    
    @Test
    @SuppressWarnings({"checkstyle:EmptyLineSeparator", "checkstyle:RightCurlyAlone", "checkstyle:NeedBraces"})
    void testReduce1() throws ParseException {
        ParseNode tree = PARSER.parse("2+x*-y");
        
        /**
         * This listener prints out <code>a+b</code> like <code>PLUS(a, b)</code>.
         */
        Listener listener = new Listener() {
            private final StringBuilder str = new StringBuilder();
            
            @Override
            public String toString() {
                return str.toString();
            }

            @Override
            public Characteristics characteristics() {
                return new Characteristics() {
                    @Override
                    public UnaryOperatorPosition unaryOperatorPosition() {
                        return UnaryOperatorPosition.OPERATOR_FIRST;
                    }
                    
                    @Override
                    public BinaryOperatorPosition binaryOperatorPosition() {
                        return BinaryOperatorPosition.OPERATOR_FIRST;
                    }
                    
                    @Override
                    public FunctionPosition functionPosition() {
                        return FunctionPosition.FUNCTION_FIRST;
                    }
                };
            }
            
            @Override public void startBinaryOperator(BinaryOperatorNode operator) { }
            @Override public void acceptBinaryOperator(BinaryOperatorNode operator) { str.append(operator.getClass().getSimpleName()).append('('); }
            @Override public void nextBinaryOperatorArgument(BinaryOperatorNode operator) { str.append(", "); }
            @Override public void endBinaryOperator(BinaryOperatorNode operator) { str.append(')'); }
            
            @Override public void startUnaryOperator(UnaryOperatorNode operator) { }
            @Override public void acceptUnaryOperator(UnaryOperatorNode operator) { str.append(operator.getToken()); }
            @Override public void endUnaryOperator(UnaryOperatorNode operator) { }
            
            @Override public void startFunction(FunctionNode function) { }
            @Override public void acceptFunction(FunctionNode function) { str.append(function.getName()).append('('); }
            @Override public void nextFunctionArgument(FunctionNode operator, int argNumber) { if (argNumber > 0) str.append(", "); }
            @Override public void endFunction(FunctionNode function) { str.append(')'); }
                    
            @Override public void startLiteral(LiteralNode literal) { }
            @Override public void acceptLiteral(LiteralNode literal) { str.append(literal.toString()); }
            @Override public void endLiteral(LiteralNode literal) { }
                    
            @Override public void startIdentifier(IdentifierNode identifier) { }
            @Override public void acceptIdentifier(IdentifierNode identifier) {  str.append("table.").append(map(identifier.getIdentifier())); }
            @Override public void endIdentifier(IdentifierNode identifier) { }
            
            private String map(String identifier) {
                return switch (identifier) {
                    case "x" -> "a";
                    case "y" -> "b";
                    default -> throw new UnsupportedOperationException(identifier);
                };
            }
        };
        
        tree.reduce(listener);
        String summary = listener.toString();
        assertEquals("PLUS(2, TIMES(table.a, -table.b))", summary);
    }

    @Test
    @SuppressWarnings({"checkstyle:EmptyLineSeparator", "checkstyle:RightCurlyAlone", "checkstyle:NeedBraces"})
    void testReduce2() throws ParseException {
        ParseNode tree = PARSER.parse("3 + (min(5, x) + -y) * z");
        
        /**
         * This listener prints out the original expression.
         * 
         * <p>We can use this approach to convert a query expression into a SQL where clause.
         * acceptIdentifier would append the column name corresponding to the identifier.
         * But if the expression were "a ** b", this would need to converted to "pow(colA, colB)", which
         * the approach here does not do, as it visits the 'a' before the '**', because of OPERATOR_MIDDLE.
         * To solve, we could try the approach below.
         */
        Listener listener = new Listener() {
            private final StringBuilder str = new StringBuilder();
            
            @Override
            public String toString() {
                return str.toString();
            }

            @Override
            public Characteristics characteristics() {
                return new Characteristics() {
                    @Override
                    public UnaryOperatorPosition unaryOperatorPosition() {
                        return UnaryOperatorPosition.OPERATOR_FIRST;
                    }
                    
                    @Override
                    public BinaryOperatorPosition binaryOperatorPosition() {
                        return BinaryOperatorPosition.OPERATOR_MIDDLE;
                    }
                    
                    @Override
                    public FunctionPosition functionPosition() {
                        return FunctionPosition.FUNCTION_FIRST;
                    }
                };
            }
            
            @Override public void startBinaryOperator(BinaryOperatorNode operator) { if (operator.isAtomic()) str.append('('); }
            @Override public void acceptBinaryOperator(BinaryOperatorNode operator) { str.append(operator.getToken()); }
            @Override public void endBinaryOperator(BinaryOperatorNode operator) { if (operator.isAtomic()) str.append(')'); }
            
            @Override public void acceptUnaryOperator(UnaryOperatorNode operator) { str.append(operator.getToken()); }
            
            @Override public void acceptFunction(FunctionNode function) { str.append(function.getName()).append('('); }
            @Override public void nextFunctionArgument(FunctionNode operator, int argNumber) { if (argNumber > 0) str.append(","); }
            @Override public void endFunction(FunctionNode function) { str.append(')'); }
                    
            @Override public void acceptLiteral(LiteralNode literal) { str.append(literal.toString()); }
            
            @Override public void acceptIdentifier(IdentifierNode identifier) {  str.append(identifier.getIdentifier()); }
        };
        
        tree.reduce(listener);
        String summary = listener.toString();
        assertEquals("3+(min(5,x)+-y)*z", summary);
    }

    @Test
    void testReduce3() throws ParseException {
        ParseNode tree = PARSER.parse("3 + (min(5, x) + -y) * z");
        
        /**
         * This listener evaluates the parse tree using stacks.
         *
         * <p>We can use this approach to build a JOOQ where clause -- that is, to reduce the expression
         * to a org.jooq.Condition.
         */
        class EvalListener implements Listener {
            private final Stack<String> binaryOperators = new Stack<>();
            private final Stack<String> unaryOperators = new Stack<>();
            private final Stack<String> functions = new Stack<>();
            private final Stack<Integer> values = new Stack<>();
            
            public int result() {
                assertEquals(1, values.size());
                return values.peek();
            }

            @Override
            public Characteristics characteristics() {
                return new Characteristics() {
                    @Override
                    public UnaryOperatorPosition unaryOperatorPosition() {
                        return UnaryOperatorPosition.OPERATOR_FIRST;
                    }
                    
                    @Override
                    public BinaryOperatorPosition binaryOperatorPosition() {
                        return BinaryOperatorPosition.OPERATOR_FIRST;
                    }
                    
                    @Override
                    public FunctionPosition functionPosition() {
                        return FunctionPosition.FUNCTION_FIRST;
                    }
                };
            }
            
            @Override
            public void startBinaryOperator(BinaryOperatorNode operator) {
                binaryOperators.push(operator.getToken());
            }
            
            @Override
            public void endBinaryOperator(BinaryOperatorNode operator) {
                int second = values.pop();
                int first = values.pop();
                switch (binaryOperators.pop()) {
                    case "+" -> values.push(first + second);
                    case "*" -> values.push(first * second);
                    default -> throw new UnsupportedOperationException();
                }
            }
            
            @Override
            public void startUnaryOperator(UnaryOperatorNode operator) {
                unaryOperators.push(operator.getToken());
            }

            @Override
            public void endUnaryOperator(UnaryOperatorNode operator) {
                int value = values.pop();
                if ("-".equals(unaryOperators.pop())) {
                    values.push(-value);
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            
            @Override
            public void startFunction(FunctionNode function) {
                functions.push(function.getName());
            }
            
            @Override
            public void endFunction(FunctionNode function) {
                if ("min".equals(functions.pop())) {
                    int first = values.pop();
                    int second = values.pop();
                    values.push(Math.min(first, second));
                } else {
                    throw new UnsupportedOperationException();
                }
            }
                    
            @Override
            public void acceptLiteral(LiteralNode literal) {
                values.push((int) literal.getValue());
            }
                    
            @Override
            public void acceptIdentifier(IdentifierNode identifier) {
                values.push(map(identifier.getIdentifier()));
            }
            
            private int map(String identifier) {
                return switch (identifier) {
                    case "x" -> 1;
                    case "y" -> 2;
                    case "z" -> 3;
                    default -> throw new UnsupportedOperationException(identifier);
                };
            }
            
        }
        
        EvalListener listener = new EvalListener();
        tree.reduce(listener);
        assertEquals(0, listener.result());
    }

    @SuppressWarnings("checkstyle:Indentation") // to suppress complaints about entry -> entry.getValue().getClass() below
    private static int evaluate(String expression, Map<String, Object> scope) throws ParseException {
        ParseNode tree = PARSER.parse(expression);
        Map<String, Class<?>> scopeTypes = scope.entrySet()
                                                .stream()
                                                .collect(Collectors.toMap(Map.Entry::getKey,
                                                                          entry -> entry.getValue().getClass()));
        assertEquals(Integer.class, tree.checkEval(scopeTypes));
        return (int) tree.eval(scope);
    }
    
    private static void assertParseError(String expression, String expectedErrorMsg, int expectedOffset) {
        assertExceptionFromCallable(
            () -> PARSER.parse(expression),
            ParseException.class,
            pe -> {
                assertEquals(expectedErrorMsg, pe.getMessage());
                assertEquals(expectedOffset, pe.getErrorOffset());
            });
    }
    
    /////
    /////
    
    private static final NumberFactory NUMBER_FACTORY = UnitNumberFactory.builder()
                                                                         .setUnitCase(StringCase.ACTUAL_CASE)
                                                                         .addUnit("m", val -> val)
                                                                         .addUnit("km", ExpressionParserTest::multiplyTimesOneThousand)
                                                                         .setDefaultUnit("m")
                                                                         .setUnitPosition(UnitPosition.AFTER)
                                                                         .build();
    
    private static Integer multiplyTimesOneThousand(Number number) {
        return number.intValue() * 1000;
    }
    
    private static final ExpressionParser PARSER = ExpressionParser.builder()
                                                                   .setNumberFactory(NUMBER_FACTORY)
                                                                   .addBinaryOperator(PLUS.class)
                                                                   .addBinaryOperator(MINUS.class)
                                                                   .addBinaryOperator(TIMES.class)
                                                                   .addBinaryOperator(DIVIDE.class)
                                                                   .addUnaryOperator(POSITIVE.class)
                                                                   .addUnaryOperator(NEGATIVE.class)
                                                                   .setFunctionCase(StringCase.ALL_LETTERS_SAME_CASE)
                                                                   .addFunction(MAX.class)
                                                                   .addFunction(MIN.class)
                                                                   .build();
    
    /////

    private abstract static class ArithmeticIntegerBinaryOperator extends BinaryOperatorNode {
        @Override
        protected final Class<?> checkCombine(Class<?> left, Class<?> right) throws TypeException {
            TypeException.assertSameType("+", 0, left, right);
            if (!left.equals(Integer.class)) {
                throw new TypeException("only integer types are supported");
            }
            return left;
        }
        
        @Override
        protected final Integer combine(Object left, Object right) throws EvalException {
            return doCombine((int)left, (int)right);
        }

        protected abstract int doCombine(int left, int right);        
    }

    public static final class PLUS extends ArithmeticIntegerBinaryOperator {
        @Override
        public String getToken() {
            return "+";
        }

        @Override
        public int getPrecedence() {
            return 1;
        }

        @Override
        protected int doCombine(int left, int right) {
            return left + right;
        }        
    }
    
    public static class MINUS extends ArithmeticIntegerBinaryOperator {
        @Override
        public String getToken() {
            return "-";
        }

        @Override
        public int getPrecedence() {
            return 1;
        }

        @Override
        protected int doCombine(int left, int right) {
            return left - right;
        }        
    }
    
    public static class TIMES extends ArithmeticIntegerBinaryOperator {
        @Override
        public String getToken() {
            return "*";
        }

        @Override
        public int getPrecedence() {
            return 2;
        }

        @Override
        protected int doCombine(int left, int right) {
            return left * right;
        }        
    }
    
    public static class DIVIDE extends ArithmeticIntegerBinaryOperator {
        @Override
        public String getToken() {
            return "/";
        }

        @Override
        public int getPrecedence() {
            return 2;
        }

        @Override
        protected int doCombine(int left, int right) {
            return left / right;
        }        
    }
    
    /////
    
    private abstract static class ArithmeticIntegerUnaryOperator extends UnaryOperatorNode {
        @Override
        protected final Class<?> checkApply(Class<?> type) throws TypeException {
            if (!type.equals(Integer.class)) {
                throw new TypeException("only integer types are supported");
            }
            return type;
        }

        @Override
        protected final Integer apply(Object value) throws EvalException {
            return doApply((int) value);
        }

        protected abstract int doApply(int value);
    }
    
    public static class POSITIVE extends ArithmeticIntegerUnaryOperator {
        @Override
        public String getToken() {
            return "+";
        }

        @Override
        protected int doApply(int value) {
            return value;
        }
    }    
    
    public static class NEGATIVE extends ArithmeticIntegerUnaryOperator {
        @Override
        public String getToken() {
            return "-";
        }

        @Override
        protected int doApply(int value) {
            return -value;
        }
    }
    
    /////
    
    private abstract static class ArithmeticIntegerFunction extends FunctionNode {
        @Override
        protected final Class<?> checkCombine(List<Class<?>> args) throws TypeException {
            if (!args.isEmpty()) {
                Class<?> type = args.get(0);
                if (!type.equals(Integer.class)) {
                    throw new TypeException("only integer types are supported");
                }
                for (Class<?> other : args) {
                    TypeException.assertSameType(getName(), 0, type, other);
                }
            }
            return Integer.class;
        }

        @Override
        protected final Integer combine(List<Object> args) throws EvalException {
            return doApply(args.stream().map(object -> (int)object));
        }
        
        protected abstract int doApply(Stream<Integer> values);
    }
    
    public static class MAX extends ArithmeticIntegerFunction {
        @Override
        protected String getName() {
            return "max";
        }

        @Override
        protected int getMinArgs() {
            return 2;
        }

        @Override
        protected int getMaxArgs() {
            return 3;
        }
        
        @Override
        protected int doApply(Stream<Integer> values) {
            return values.max(Comparator.naturalOrder())
                         .get();
        }
    }
    
    public static class MIN extends ArithmeticIntegerFunction {
        @Override
        protected String getName() {
            return "min";
        }

        @Override
        protected int getMinArgs() {
            return 2;
        }

        @Override
        protected int getMaxArgs() {
            return 3;
        }

        @Override
        protected int doApply(Stream<Integer> values) {
            return values.min(Comparator.naturalOrder())
                         .get();
        }
    }
}
