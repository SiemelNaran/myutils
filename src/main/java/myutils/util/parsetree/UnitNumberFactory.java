package myutils.util.parsetree;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class UnitNumberFactory implements NumberFactory {    
    public static Builder builder() {
        return new Builder();
    }
    
    private final @Nonnull DefaultNumberFactory numberFactory;
    private final @Nonnull UnitPosition unitPosition;
    private final @Nonnull Map<String /*unit*/, UnaryOperator<Number>> units;
    private final @Nullable UnaryOperator<Number> defaultConverter;
    
    private UnitNumberFactory(DefaultNumberFactory numberFactory,
                              UnitPosition unitPosition,
                              Map<String /*unit*/, UnaryOperator<Number>> units,
                              UnaryOperator<Number> defaultConverter) {
        this.numberFactory = numberFactory;
        this.unitPosition = unitPosition;
        this.units = units;
        this.defaultConverter = defaultConverter;
    }

    @Override
    public Number fromString(String str) throws NumberFormatException {
        if (str == null) {
            return null;
        }
        
        String original = str;
        
        String unitName;
        switch (unitPosition) {
            case BEFORE: {
                int[] unit = getWordAtStart(str);
                if (unit.length > 0) {
                    unitName = new String(unit, 0, unit.length);
                    str = str.substring(unitName.length());
                } else {
                    unitName = "";
                }
                break;
            }
            
            case AFTER: {
                int[] number = getNumberAtStart(str);
                if (number.length > 0) {
                    unitName = str.substring(number.length);
                    str = new String(number, 0, number.length);
                } else {
                    unitName = "";
                }
                break;
            }
                
            default:
                throw new UnsupportedOperationException();
        }
        
        UnaryOperator<Number> converter;
        if (unitName.isEmpty()) {
            converter = defaultConverter;
            if (converter == null) {
                throw new NumberFormatException("unit missing in " + original);
            }
        } else {
            converter = units.get(unitName);
            if (converter == null) {
                throw new NumberFormatException("unrecognized unit " + unitName + " in " + original);
            }
        }
        
        Number basic = numberFactory.fromString(str);
        return converter.apply(basic);
    }
    
    private static @Nonnull int[] getWordAtStart(String str) {
        return str.codePoints().takeWhile(Character::isLetter).toArray();
    }
    
    private static @Nonnull int[] getNumberAtStart(String str) {
        return str.codePoints().takeWhile(UnitNumberFactory::isNumberChar).toArray();
    }
    
    private static boolean isNumberChar(int c) {
        return Character.isDigit(c) || c == '.' || c == '+' || c == '-';
    }
    
    public enum UnitPosition {
        BEFORE,
        AFTER
    }

    public static class Builder {
        private @Nonnull DefaultNumberFactory numberFactory = DefaultNumberFactory.DEFAULT_NUMBER_FACTORY;
        private UnitPosition unitPosition = UnitPosition.AFTER;
        private @Nonnull final Map<String /*unit*/, UnaryOperator<Number>> units = new HashMap<>();
        private @Nullable String defaultUnit;
        
        /**
         * Set the factory used to parse the number part of a string, such as the "3" in "3km".
         * 
         * @param numberFactory the number factory
         * @return this
         */
        public Builder setNumberFactory(@Nonnull DefaultNumberFactory numberFactory) {
            this.numberFactory = Objects.requireNonNull(numberFactory);
            return this;
        }
        
        /**
         * Set the unit position, as in after the number or before or either.
         * 
         * @param unitPosition the unit position
         * @return this
         */
        public Builder setUnitPosition(@Nonnull UnitPosition unitPosition) {
            this.unitPosition = Objects.requireNonNull(unitPosition);
            return this;
        }
        
        /**
         * Add a unit.
         * 
         * @param unit the unit abbreviation
         * @param converter a tool to convert the plain Number read by the number factory to a new number
         * @return this
         * @throws IllegalArgumentException if unit name is not all letters
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
                                         unitPosition,
                                         units,
                                         units.get(defaultUnit));
        }
    }
}
