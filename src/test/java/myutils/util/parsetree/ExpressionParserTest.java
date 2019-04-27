package myutils.util.parsetree;

import static myutils.TestUtil.assertExceptionFromCallable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;


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
        assertEquals(new Integer(1_003_002), evaluate("2m+3km+x", scope));
    }
    
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
        ParseException pe = assertExceptionFromCallable(() -> PARSER.parse(expression), ParseException.class, expectedErrorMsg);
        assertEquals(expectedOffset, pe.getErrorOffset());
    }
    
    /////
    /////
    
    private static final NumberFactory NUMBER_FACTORY = UnitNumberFactory.builder()
                                                                         .addUnit("m", val -> val)
                                                                         .addUnit("km", val -> val * 1000)
                                                                         .setDefaultUnit("m")
                                                                         .setUnitAfter(true)
                                                                         .build();
    
    private static Integer multiplyTimesOneThousand(Number number) {
        return ((Integer) number).intValue * 1000;
    }
    
    private static final ExpressionParser PARSER = ExpressionParser.builder()
                                                                   .setNumberFactory(NUMBER_FACTORY)
                                                                   .addBinaryOperator(PLUS.class)
                                                                   .addBinaryOperator(MINUS.class)
                                                                   .addBinaryOperator(TIMES.class)
                                                                   .addBinaryOperator(DIVIDE.class)
                                                                   .addUnaryOperator(POSITIVE.class)
                                                                   .addUnaryOperator(NEGATIVE.class)
                                                                   .addFunction(MAX.class)
                                                                   .addFunction(MIN.class)
                                                                   .build();
    
    /////

    private static abstract class ArithmeticIntegerBinaryOperator extends BinaryOperatorNode {
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
    
    private static abstract class ArithmeticIntegerUnaryOperator extends UnaryOperatorNode {
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
    
    private static abstract class ArithmeticIntegerFunction extends FunctionNode {
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
        
        protected int doApply(Stream<Integer> values) {
            return values.collect(Collectors.maxBy(Comparator.<Integer>naturalOrder()))
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
        
        protected int doApply(Stream<Integer> values) {
            return values.collect(Collectors.minBy(Comparator.<Integer>naturalOrder()))
                         .get();
        }
    }
}
