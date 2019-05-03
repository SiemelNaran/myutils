package myutils.util.parsetree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class UnitNumberFactory implements NumberFactory {    
    public static Builder builder() {
        return new Builder();
    }
    
    private final @Nonnull DefaultNumberFactory numberFactory;
    private final boolean unitAfter;
    private final @Nonnull Collection<Map.Entry<String /*unit*/, UnaryOperator<Number>>> units; // sorted by longest first
    private final @Nullable UnaryOperator<Number> defaultConverter;
    
    private UnitNumberFactory(DefaultNumberFactory numberFactory,
                              boolean unitAfter,
                              Collection<Map.Entry<String /*unit*/, UnaryOperator<Number>>> units,
                              UnaryOperator<Number> defaultConverter) {
        this.numberFactory = numberFactory;
        this.unitAfter = unitAfter;
        this.units = units;
        this.defaultConverter = defaultConverter;
    }

    @Override
    public Number fromString(String str) throws NumberFormatException {
        if (str == null) {
            return null;
        }
        
        UnaryOperator<Number> converter = defaultConverter;
        if (unitAfter) {
            for (Map.Entry<String, UnaryOperator<Number>> entry : units) {
                String unitName = entry.getKey();
                if (str.endsWith(unitName)) {
                    converter = entry.getValue();
                    str = str.substring(0, str.length() - unitName.length());
                }
            }
        } else {
            for (Map.Entry<String, UnaryOperator<Number>> entry : units) {
                String unitName = entry.getKey();
                if (str.startsWith(unitName)) {
                    converter = entry.getValue();
                    str = str.substring(unitName.length());
                }
            }
        }
        if (converter == null) {
            throw new NumberFormatException("unrecognized unit in " + str);
        }
        
        Number basic = numberFactory.fromString(str);
        return converter.apply(basic);
    }
    
    public static class Builder {
        private @Nonnull DefaultNumberFactory numberFactory = DefaultNumberFactory.DEFAULT_NUMBER_FACTORY;
        private boolean unitAfter = true;
        private @Nonnull final Map<String /*unit*/, UnaryOperator<Number>> units = new HashMap<>();
        private @Nullable String defaultUnit;
        
        public Builder setNumberFactory(@Nonnull DefaultNumberFactory numberFactory) {
            this.numberFactory = numberFactory;
            return this;
        }
        
        public Builder setUnitAfter(boolean unitAfter) {
            this.unitAfter = unitAfter;
            return this;
        }
        
        /**
         * Add a unit.
         * 
         * @param unit the unit abbreviation
         * @param converter a tool to convert the plain Number read by the number factory to a new number
         * @return this
         */
        public Builder addUnit(@Nonnull String unit, UnaryOperator<Number> converter) {
            verifyUnit(unit);
            this.units.put(unit, converter);
            return this;
        }
        
        private static void verifyUnit(String unit) {
            int len = unit.length();
            for (int i = 0; i < len; i++) {
                char c = unit.charAt(i);
                if (!Character.isLetter(c)) {
                    throw new IllegalArgumentException("invalid unit " + unit);
                }
            }

        }
        
        /**
         * Set the default unit. null means that a unit is required when parsing a number.
         */
        public Builder setDefaultUnit(String defaultUnit) {
            if (!units.containsKey(defaultUnit)) {
                throw new IllegalArgumentException("default unit " + defaultUnit + " not found");
            }
            this.defaultUnit = defaultUnit;
            return this;
        }
        
        /**
         * Build the unit number factory.
         */
        public UnitNumberFactory build() {
            return new UnitNumberFactory(numberFactory,
                                         unitAfter,
                                         getUnits(),
                                         units.get(defaultUnit));
        }
        
        private List<Map.Entry<String, UnaryOperator<Number>>> getUnits() {
            var list = new ArrayList<>(units.entrySet());
            list.sort((first, second) -> second.getKey().length() - first.getKey().length());
            return list;
        }
    }
}
