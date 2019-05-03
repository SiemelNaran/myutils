package myutils.util.parsetree;

import static myutils.TestUtil.assertException;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;


public class UnitNumberFactoryTest {
    @Test
    public void testDefaultNumberFactoryAfter() {
        NumberFactory factory = UnitNumberFactory.builder()
                                                 .addUnit("m", val -> val)
                                                 .addUnit("km", val -> val.intValue() * 1000)
                                                 .setDefaultUnit("m")
                                                 .setUnitAfter(true)
                                                 .build();
        
        assertEquals(Integer.valueOf(2), factory.fromString("2"));
        assertEquals(Integer.valueOf(2), factory.fromString("2m"));
        assertEquals(Integer.valueOf(2000), factory.fromString("2km"));
    }

    @Test
    public void testDefaultNumberFactoryBefore() {
        NumberFactory factory = UnitNumberFactory.builder()
                                                 .setNumberFactory(DefaultNumberFactory.builder()
                                                                                       .setIntegerPolicy(null)
                                                                                       .setFloatPolicy(DefaultNumberFactory.FloatPolicy.PREFER_BIG_DECIMAL)
                                                                                       .build())
                                                 .addUnit("USD", val -> new USD((BigDecimal) val))
                                                 .addUnit("EUR", val -> new EUR((BigDecimal) val))
                                                 .setUnitAfter(false)
                                                 .build();
        
        assertEquals(new USD(new BigDecimal("2.00")), factory.fromString("USD2"));
        assertEquals(new EUR(new BigDecimal("4.00")), factory.fromString("EUR4"));
        assertException(() -> factory.fromString("2"), NumberFormatException.class, "unit missing in 2");
    }
    
    private static class Currency extends Number {
        private static final long serialVersionUID = 1L;

        private final BigDecimal val;
        
        protected Currency(BigDecimal val) {
            this.val = val;
        }
        
        @Override
        public int intValue() {
            return val.intValue();
        }

        @Override
        public long longValue() {
            return val.longValue();
        }

        @Override
        public float floatValue() {
            return val.floatValue();
        }

        @Override
        public double doubleValue() {
            return val.doubleValue();
        }
    }
    
    private static class USD extends Currency {
        private static final long serialVersionUID = 1L;

        public USD(BigDecimal val) {
            super(val);
        }
    }
    
    private static class EUR extends Currency {
        private static final long serialVersionUID = 1L;
        
        public EUR(BigDecimal val) {
            super(val);
        }
    }
}
