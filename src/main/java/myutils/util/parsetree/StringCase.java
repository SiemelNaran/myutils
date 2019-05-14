package myutils.util.parsetree;

import javax.annotation.Nonnull;


public enum StringCase {
    /**
     * Respect the original case of the function, so max and MAX are different functions.
     */
    ACTUAL_CASE {
        @Override
        @Nonnull String convert(@Nonnull String str) {
            return str;
        }
    },
    
    /**
     * Ignore the original case of the function, so max and MAX and Max are the same function.
     */
    IGNORE_CASE {
        @Override
        @Nonnull String convert(@Nonnull String str) {
            return str.toLowerCase();
        }
    },
    
    ALL_LETTERS_SAME_CASE {                
        @Override
        @Nonnull String convert(@Nonnull String str) {
            if (!str.isEmpty()) {
                int first = str.codePointAt(0);
                // verifyFunctionNameValid verifies first char is a letter
                // so one of the below two conditions must be true
                if (Character.isLowerCase(first)) {
                    str.codePoints().skip(1).forEach(StringCase::assertLowerCase);
                } else if (Character.isUpperCase(first)) {
                    str.codePoints().skip(1).forEach(StringCase::assertUpperCase);
                    str = str.toLowerCase();
                }
            }
            return str;
        }
    };
    
    /**
     * Convert the function name.
     * 
     * @param str the function 
     * @return the converted function
     * @throws IllegalArgumentException if the function case is invalid
     */
    abstract String convert(@Nonnull String str);
    
    private static void assertLowerCase(int c) {
        if (Character.isLetter(c) && !Character.isLowerCase(c)) {
            throw new IllegalArgumentException("Found lowercase character in function name");
        }
    }
    
    private static void assertUpperCase(int c) {
        if (Character.isLetter(c) && !Character.isUpperCase(c)) {
            throw new IllegalArgumentException("Found uppercase character in function name");
        }
    }
}
