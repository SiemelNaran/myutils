package myutils.util.parsetree;


public class UnitNumberFactory implements NumberFactory {    
    public static Builder builder() {
        return new Builder();
    }
    
    private final @Nonnull DefaultNumberFactory numberFactory;
    private final boolean unitAfter;
    private final @Nonnull List<Map.Entry<String /*unit*/, UnaryFunction<Number>>> units; // sorted by longest first
    private final @Nullable String defaultConverter;
    
    private UnitNumberFactory(DefaultNumberFactory numberFactory,
                              List<Map.Entry<String /*unit*/, UnaryFunction<Number>>> units,
                              UnaryFunction<Number> defaultConverter) {
        this.numberFactory = numberFactory;
        this.units = units;
        this.defaultConverter = defaultConverter;
    }

    @Override
    public Number fromString(String str) throws NumberFormatException {
        if (str == null) {
            return null;
        }
        
        UnaryFunction<Number> converter = defaultConverter;
        if (unitAfter) {
            for (Map.Entry<String, UnaryFunction<Number>> units : units) {
                String unitName = entry.getKey();
                if (number.endsWith(unitName)) {
                    converter = entry.getValue();
                    str = str.substring(0, str.length() - unitName.length());
                }
            }
        } else {
            for (Map.Entry<String, UnaryFunction<Number>> units : units) {
                String unitName = entry.getKey();
                if (number.startsWith(unitName)) {
                    converter = entry.getValue();
                    str = str.substring(unitName.length());
                }
            }
        }
        if (converter == null) {
            throw new NumberFormatException("unit missing in " + str);
        }
        
        Number basic = numberFactory.fromString(str);
        return converter.apply(basic);
    }
    
    public static class Builder {
        private @Nonnull DefaultNumberFactory numberFactory = DefaultNumberFactory.DEFAULT_NUMBER_FACTORY;
        private @Nonnull final Map<String /*unit*/, UnaryFunction<Number>> units;
        private @Nullable String defaultUnit;
        
        public Builder setNumberFactory(@Nonnull DefaultNumberFactory numberFactory) {
            this.numberFactory = numberFactory;
            return this;
        }
        
        public Builder addUnit(@Nonnull String unit, UnaryFunction<Number> converter) {
            verifyUnit(unit);
            this.units.put(unit, converter);
            return this;
        }
        
        private static void verifyUnit(String unit) {
            int N = unit.length();
            for (int i = 0; i < N; i++) {
                char c = oper.charAt(i);
                if (!Character.isLetter(c)) {
                    throw new IllegalArgumentException("invalid unit " + unit);
                }
            }
            return true;

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
        
        public UnitNumberFactory build() {
            return new UnitNumberFactory(numberFactory,
                                         new ArrayList<>(),
                                         defaultUnit);
        }
    }
}
