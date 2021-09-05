package org.sn.myutils.parsetree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.sn.myutils.testutils.TestUtil.assertException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.sn.myutils.parsetree.UnitNumberFactory.UnitPosition;


public class UnitNumberFactoryTest {
    @Test
    void testDefaultNumberFactoryAfter() {
        NumberFactory factory = UnitNumberFactory.builder()
                                                 .setUnitCase(StringCase.IGNORE_CASE)
                                                 .addUnit("m", val -> val)
                                                 .addUnit("km", val -> val.intValue() * 1000)
                                                 .setDefaultUnit("M")
                                                 .setUnitPosition(UnitPosition.AFTER)
                                                 .build();
        
        assertEquals(Integer.valueOf(2), factory.fromString("2"));
        assertEquals(Integer.valueOf(2), factory.fromString("2m"));
        assertEquals(Integer.valueOf(2000), factory.fromString("2km"));
        assertEquals(Integer.valueOf(2000), factory.fromString("2kM"));
        assertException(() -> factory.fromString("km2"),
                        NumberFormatException.class,
                        "Character k is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.");
    }

    @Test
    void testDefaultNumberFactoryBefore() {
        NumberFactory factory = UnitNumberFactory.builder()
                                                 .setNumberFactory(DefaultNumberFactory.builder()
                                                                                       .setIntegerPolicy(null)
                                                                                       .setFloatPolicy(DefaultNumberFactory.FloatPolicy.PREFER_BIG_DECIMAL)
                                                                                       .setBigDecimalScale(2, RoundingMode.FLOOR)
                                                                                       .build())
                                                 .setUnitCase(StringCase.ACTUAL_CASE)
                                                 .addUnit("USD", val -> new USD((BigDecimal) val))
                                                 .addUnit("EUR", val -> new EUR((BigDecimal) val))
                                                 .setUnitPosition(UnitPosition.BEFORE)
                                                 .build();
        
        assertEquals(new USD(new BigDecimal("2.00")), factory.fromString("USD2"));
        assertEquals(new EUR(new BigDecimal("4.00")), factory.fromString("EUR4"));
        assertEquals(new EUR(new BigDecimal("4.05")), factory.fromString("EUR4.05"));
        assertException(() -> factory.fromString("2"), NumberFormatException.class, "unit missing in 2");
        assertException(() -> factory.fromString("usd2"), NumberFormatException.class, "unrecognized unit usd in usd2");
        assertException(() -> factory.fromString("XYZ2"), NumberFormatException.class, "unrecognized unit XYZ in XYZ2");
        assertEquals(new EUR(new BigDecimal("4.05")), factory.fromString("EUR4.05"));
    }
    
    private abstract static class Currency extends Number {
        private static final long serialVersionUID = 1L;

        private final BigDecimal val;
        
        protected Currency(BigDecimal val) {
            this.val = val;
        }
        
        protected final BigDecimal rawvalue() {
            return val;
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
        
        @Override
        public abstract String toString();
        
        @Override
        public abstract boolean equals(Object thatObject);
        
        @Override
        public abstract int hashCode();
    }
    
    private static final class USD extends Currency {
        private static final long serialVersionUID = 1L;

        public USD(BigDecimal val) {
            super(val);
        }
        
        @Override
        public String toString() {
            return "USD" + Objects.toString(rawvalue());
        }
        
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof USD that)) {
                return false;
            }
            return Objects.equals(this.rawvalue(), that.rawvalue());
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(rawvalue());
        }
    }
    
    private static final class EUR extends Currency {
        private static final long serialVersionUID = 1L;
        
        public EUR(BigDecimal val) {
            super(val);
        }
        
        @Override
        public String toString() {
            return "EUR" + Objects.toString(rawvalue());
        }
        
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof EUR that)) {
                return false;
            }
            return Objects.equals(this.rawvalue(), that.rawvalue());
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(rawvalue());
        }
    }
}
