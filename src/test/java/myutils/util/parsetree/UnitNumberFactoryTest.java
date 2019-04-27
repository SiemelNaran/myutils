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


public class UnitNumberFactory Test {
    @Test
    public void testDefaultNumberFactoryAfter() {
        NumberFactory factory = UnitNumberFactory.builder()
                                                 .addUnit("m", val -> val)
                                                 .addUnit("km", val -> val.intValue() * 1000)
                                                 .setDefaultUnit("m")
                                                 .setUnitAfter(true)
                                                 .build();
        
        assertEquals(new Integer(2), factory.fromString("2"));
        assertEquals(new Integer(2), factory.fromString("2m"));
        assertEquals(new Integer(2000), factory.fromString("2km"));
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
        
        assertEquals(new USD(2), factory.fromString("USD2"));
        assertEquals(new EUR(2), factory.fromString("EUR4"));
        assertException(() -> factory.fromString("2"), NumberFormatException.class, "unit missing in 2");
    }
    
    private interface Currency {        
    }
    
    private static class USD implements Currency, Number {
        private final BigDecimal value;
        
        public USD(BigDecimal value) {
            this.value = value;
        }
    }
    
    private static class EUR implements Currency, Number {
        private final BigDecimal value;
        
        public EUR(BigDecimal value) {
            this.value = value;
        }
    }
}
