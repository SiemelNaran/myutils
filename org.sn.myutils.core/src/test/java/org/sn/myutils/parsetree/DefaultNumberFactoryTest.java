package org.sn.myutils.parsetree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.sn.myutils.testutils.TestUtil.assertException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.junit.jupiter.api.Test;


public class DefaultNumberFactoryTest {
    @Test
    public void testDefaultNumberFactory() {
        NumberFactory factory = DefaultNumberFactory.DEFAULT_NUMBER_FACTORY;
        
        assertEquals(Integer.valueOf(33), factory.fromString("33"));
        assertEquals(Integer.valueOf(33), factory.fromString("+33"));
        assertEquals(Integer.valueOf(-33), factory.fromString("-33"));
        assertEquals(Long.valueOf(12345678901L), factory.fromString("12345678901"));
        
        assertEquals(Double.valueOf(33.7), factory.fromString("33.7"));
        assertEquals(Double.valueOf(33.7), factory.fromString("+33.7"));
        assertEquals(Double.valueOf(-33.7), factory.fromString("-33.7"));
        
        assertException(() -> factory.fromString("++33"), NumberFormatException.class);
    }
    
    @Test
    public void testDefaultNumberFactoryLong() {
        NumberFactory factory = DefaultNumberFactory.builder()
                                                    .setIntegerPolicy(DefaultNumberFactory.IntegerPolicy.PREFER_LONG)
                                                    .build();
        
        assertEquals(Long.valueOf(33), factory.fromString("33"));
        assertEquals(Long.valueOf(33), factory.fromString("+33"));
        assertEquals(Long.valueOf(-33), factory.fromString("-33"));
        assertEquals(Long.valueOf(12345678901L), factory.fromString("12345678901"));
        
        assertEquals(Double.valueOf(33.7), factory.fromString("33.7"));
        assertEquals(Double.valueOf(33.7), factory.fromString("+33.7"));
        assertEquals(Double.valueOf(-33.7), factory.fromString("-33.7"));
    }
    
    @Test
    public void testDefaultNumberFactoryTwoDecimalPlaces() {
        NumberFactory factory = DefaultNumberFactory.builder()
                                                    .setIntegerPolicy(null)
                                                    .setFloatPolicy(DefaultNumberFactory.FloatPolicy.PREFER_BIG_DECIMAL)
                                                    .setBigDecimalScale(2, RoundingMode.HALF_UP)
                                                    .build();
        
        assertEquals(new BigDecimal("33.00"), factory.fromString("33"));
        assertEquals(new BigDecimal("33.00"), factory.fromString("+33"));
        assertEquals(new BigDecimal("-33.00"), factory.fromString("-33"));
        assertEquals(new BigDecimal("12345678901.00"), factory.fromString("12345678901"));
        
        assertEquals(new BigDecimal("33.70"), factory.fromString("33.7"));
        assertEquals(new BigDecimal("33.70"), factory.fromString("+33.7"));
        assertEquals(new BigDecimal("-33.70"), factory.fromString("-33.7"));
        
        assertEquals(new BigDecimal("33.01"), factory.fromString("33.005"));
    }
}