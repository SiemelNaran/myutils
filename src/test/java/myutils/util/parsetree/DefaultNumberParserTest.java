package myutils.util.parsetree;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;


public class NumberParserTest {
    @Test
    public void testDefaultNumberFactory() {
        NumberFactory factory = DefaultNumberFactory.builder()
                .                                   .build();
        
        assertEquals(new Integer(33), factory.fromString("33"));
        assertEquals(new Integer(33), factory.fromString("+33"));
        assertEquals(new Integer(-33), factory.fromString("-33"));
        assertEquals(new Long(12345678901L), factory.fromString("12345678901"));
        
        assertEquals(new Double(33.7), factory.fromString("33.7"));
        assertEquals(new Double(33.7), factory.fromString("+33.7"));
        assertEquals(new Double(-33.7), factory.fromString("-33.7"));
        
        assertException(() -> factory.fromString("++33"), NumberFormatException.class);
    }
    
    @Test
    public void testDefaultNumberFactoryLong() {
        NumberFactory factory = DefaultNumberFactory.builder()
                                                    .setIntegeryPolicy(DefaultNumberFactory.IntegerPolicy.PREFER_LONG)
                .                                   .build();
        
        assertEquals(new Long(33), factory.fromString("33"));
        assertEquals(new Long(33), factory.fromString("+33"));
        assertEquals(new Long(-33), factory.fromString("-33"));
        assertEquals(new Long(12345678901L), factory.fromString("12345678901"));
        
        assertEquals(new Double(33.7), factory.fromString("33.7"));
        assertEquals(new Double(33.7), factory.fromString("+33.7"));
        assertEquals(new Double(-33.7), factory.fromString("-33.7"));
    }
    
    @Test
    public void testDefaultNumberFactoryTwoDecimalPlaces() {
        NumberFactory factory = DefaultNumberFactory.builder()
                                                    .setIntegerPolicy(null)
                                                    .setFloatPolicy(DefaultNumberFactory.FloatPolicy.PREFER_BIG_DECIMAL)
                .                                   .build();
        
        assertEquals(new BigDecimal("33.00"), factory.fromString("33"));
        assertEquals(new BigDecimal("33.00"), factory.fromString("+33"));
        assertEquals(new BigDecimal("-33.00"), factory.fromString("-33"));
        assertEquals(new BigDecimal("12345678901.00"), factory.fromString("12345678901"));
        
        assertEquals(new BigDecimal("33.70"), factory.fromString("33.7"));
        assertEquals(new BigDecimal("33.70"), factory.fromString("+33.7"));
        assertEquals(new BigDecimal("-33.70"), factory.fromString("-33.7"));
    }
}