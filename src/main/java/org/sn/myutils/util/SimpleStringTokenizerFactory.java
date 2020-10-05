package org.sn.myutils.util;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntPredicate;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * A string tokenizer that splits a string into tokens.
 * java.util.StringTokenizer takes a list of delimiter characters (passed in as a String),
 * whereas this class takes
 * <ul>
 *   <li>predicate of skip characters</li>
 *   <li>a quoted string strategy</li>
 *   <li>a dictionary of known symbols</li>
 *   <li>a list of predicates which describe all characters in each character class</li>
 * </ul>
 *
 * <p>Skip characters are skipped and never returned.
 * 
 * <p>If the quote strategy says to support single/double quotes, then everything within the quotes,
 * including the quote characters, will be treated as one token.
 * See QuoteStrategy for more details.
 * 
 * <p>You can also pass in a dictionary of known symbols.
 * If the dictionary contains <code>*</code> and <code>**</code>, then in "2 ** 3", after reading the token "2",
 * the <code>**</code> will be treated as one token.
 * But if the dictionary contains only <code>*</code>, then the first <code>*</code> will be treated as one token,
 * and the second <code>*</code> will be treated as the next token.
 * 
 * <p>If the dictionary contains <code>*</code> and <code>***</code>, but not <code>**</code>,
 * and if string is "2 ** 3" and you've read the token "2",
 * the <code>**</code> is treated as one token even though it is not in the dictionary.
 * For this reason, the constructor throws an exception if each sub-word in the dictionary is not also in the dictionary. 
 * 
 * <p>You can also specify the list of predicates describing the skip characters.
 * If a character is not the start of a symbol, then we determine what character class describes it.
 * All characters in this character class will be considered to be part of the token.
 * For example, if the predicate is Character.isLetterOrDigit, then all letters and digits will constitute on token.
 * 
 * <p>This class parses unicode strings.
 * 
 * @author snaran
 */
public class SimpleStringTokenizerFactory {
    
    /**
     *  Class describing the quote strategy.
     *  
     *  <p>You can choose to treat text within single quotes, within double quotes, or both as a token.
     *  
     *  <p>If escape is true, characters are escaped, so if you want a double quote inside a double quoted string,
     *  enter <code>\"</code>, and if you want a backslash then enter <code>\\</code>.
     *  The tokenizer also transforms \n \r \t \f \b.
     *  
     *  <p>If escape is false, then two quotes means one quote.
     */
    public static class QuoteStrategy {
        public static Builder builder() {
            return new Builder();
        }
        
        private final boolean singleQuotes;
        private final boolean doubleQuotes;
        private final boolean escape;
        
        private QuoteStrategy(boolean singleQuotes,
                              boolean doubleQuotes,
                              boolean escape) {
            this.singleQuotes = singleQuotes;
            this.doubleQuotes = doubleQuotes;
            this.escape = escape;
        }
        
        
        public static class Builder {
            private boolean singleQuotes;
            private boolean doubleQuotes;
            private boolean escape = true;
            
            /**
             * Add a supported quote character.
             *
             * @param c must be either ' or "
             * @throws UnsupportedOperationException if c is not a supported quote character
             */
            public Builder addQuoteChar(char c) {
                if (c == '\'') {
                    singleQuotes = true;
                } else if (c == '\"') {
                    doubleQuotes = true;
                } else {
                    throw new UnsupportedOperationException();
                }
                return this;
            }
            
            /**
             * Add ' as a supported quote character.
             */
            public Builder addSingleQuoteChar() {
                return addQuoteChar('\'');
            }
            
            /**
             * Add " as a supported quote character.
             */
            public Builder addDoubleQuoteChar() {
                return addQuoteChar('"');
            }
            
            /**
             * Set whether characters will be escaped or double quoted.
             * Default is true.
             */
            public Builder setEscape(boolean escape) {
                this.escape = escape;
                return this;
            }
            
            public QuoteStrategy build() {
                return new QuoteStrategy(singleQuotes, doubleQuotes, escape);
            }
        }
    }
    
    private final IntPredicate skipChars;
    private final QuoteStrategy quoteStrategy;
    private final SimpleTrie<Integer> symbols;
    private final List<IntPredicate> characterClasses;
    private final IntPredicate otherChars;
    
    /**
     * Create a string tokenizer factory.
     * Call the function tokenizer(String) to create a tokenizer using this factory.
     * 
     * @param skipChars the characters that split one token from another, and which should not be returned
     * @param quoteStrategy the quote strategy
     * @param symbols if the token matches a symbol in this list (the longest symbol), return it
     * @param characterClasses a list of predicates, where each predicate describes the multiple characters in this character class
     * 
     * @throws IllegalArgumentException if every sub-word in symbols is not also in symbols
     */
    public SimpleStringTokenizerFactory(@Nonnull IntPredicate skipChars,
                                        @Nonnull QuoteStrategy quoteStrategy,
                                        @Nonnull List<String> symbols,
                                        @Nonnull List<IntPredicate> characterClasses) {
        this.skipChars = skipChars;
        this.quoteStrategy = quoteStrategy;
        this.symbols = buildTrie(symbols);
        this.characterClasses = characterClasses;
        this.otherChars = c -> {
            for (IntPredicate predicate : characterClasses) {
                if (predicate.test(c)) {
                    return false;
                }
            }
            if (skipChars.test(c)) {
                return false;
            }
            return true;
        };
    }
    
    private static SimpleTrie<Integer> buildTrie(List<String> symbols) {
        SimpleTrie<Integer> trie = new SimpleTrie<>();
        for (String symbol : symbols) {
            trie.add(symbol.codePoints().boxed());
        }
        checkTrie(trie);
        return trie;
    }
    
    private static void checkTrie(SimpleTrie<Integer> trie) throws IllegalArgumentException {
        trie.visit((List<Integer> word, SimpleTrie<Integer> subTrie) -> {
            if (!subTrie.isWord()) {
                int[] wordArray = word.stream().mapToInt(Integer::intValue).toArray();
                String str = new String(wordArray, 0, word.size());
                throw new IllegalArgumentException("expected to find " + str + " in dictionary");
            }
        });
    }
    
    public Iterator<Token> tokenizer(CharSequence str) {
        return new Tokenizer(str);
    }
    
    public static class Token {
        private final String token;
        private final int tokenStart;
        
        public Token(@Nonnull CharSequence token, int tokenStart) {
            this.token = token.toString(); // StringBuilder does not define equals, so using CharSequence is erroneous
            this.tokenStart = tokenStart;
        }
        
        public @Nonnull String getText() {
            return token;
        }
        
        public int getStart() {
            return tokenStart;
        }

        public int getEnd() {
            return tokenStart + token.length();
        }
    }
    
    @NotThreadSafe
    private class Tokenizer implements Iterator<Token> {
        private final CharSequence str;
        private final RewindableIterator<Integer> iterCodePoints;
        
        private Tokenizer(CharSequence str) {
            this.str = str;
            this.iterCodePoints = RewindableIterator.from(str.codePoints().iterator());
        }

        @Override
        public boolean hasNext() {
            handleSkipChars();
            return iterCodePoints.hasNext();
        }
    
        private void handleSkipChars() {
            while (iterCodePoints.hasNext()) {
                int c = iterCodePoints.next();
                if (!skipChars.test(c)) {
                    iterCodePoints.rewind();
                    break;
                }
            }
        }
        
        @Override
        public Token next() throws NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
    
            int tokenStart = iterCodePoints.getNextIndex();
            final int c = iterCodePoints.next();
            
            CharSequence token;
            if (c == '\'' && quoteStrategy.singleQuotes) {
                if (quoteStrategy.escape) {
                    token = readQuotedEscapedString(c);
                } else {
                    token = readQuotedDoubleQuotesToEscapeString(c);
                }
            } else if (c == '"' && quoteStrategy.doubleQuotes) {
                if (quoteStrategy.escape) {
                    token = readQuotedEscapedString(c);
                } else {
                    token = readQuotedDoubleQuotesToEscapeString(c);
                }
            } else {
                token = readRegularToken(tokenStart, c);
            }
            
            return new Token(token, tokenStart);
        }
        
        @SuppressWarnings("checkstyle:OneStatementPerLine")
        private CharSequence readQuotedEscapedString(final int expect) {
            StringBuilder token = new StringBuilder(32);
            token.appendCodePoint(expect);
            boolean escapeMode = false;
            while (iterCodePoints.hasNext()) {
                int c = iterCodePoints.next();
                if (!escapeMode) {
                    if (c == '\\') {
                        escapeMode = true;
                    } else {
                        token.appendCodePoint(c);
                        if (c == expect) {
                            break;
                        }
                    }
                } else {
                    switch (c) {
                        case 'n': token.append('\n'); break;
                        case 'r': token.append('\r'); break;
                        case 't': token.append('\t'); break;
                        case 'f': token.append('\f'); break;
                        case 'b': token.append('\b'); break;
                        default: token.appendCodePoint(c);
                    }
                    escapeMode = false;
                }
            }
            return token;
        }

        private CharSequence readQuotedDoubleQuotesToEscapeString(final int expect) {
            StringBuilder token = new StringBuilder(32);
            token.appendCodePoint(expect);
            boolean escapeMode = false;
            while (iterCodePoints.hasNext()) {
                int c = iterCodePoints.next();
                if (!escapeMode) {
                    if (c == expect) {
                        escapeMode = true;
                    } else {
                        token.appendCodePoint(c);
                    }
                } else {
                    token.appendCodePoint(expect);
                    if (c != expect) {
                        iterCodePoints.rewind();
                        return token;
                    }
                    escapeMode = false;
                }
            }
            if (escapeMode) {
                token.appendCodePoint(expect);
            }
            return token;
        }

        private CharSequence readRegularToken(int tokenStart, int first) {
            SimpleTrie<Integer> trie = symbols.find(first);
            if (trie != null) {
                trie = readSymbol(trie);
            } else {
                IntPredicate characterClass = determineCharacterClass(first);
                readCharacterClass(characterClass);
            }
            return str.subSequence(tokenStart, iterCodePoints.getNextIndex());
        }
        
        private @Nonnull SimpleTrie<Integer> readSymbol(@Nonnull SimpleTrie<Integer> trie) {
            while (iterCodePoints.hasNext()) {
                int c = iterCodePoints.next();
                SimpleTrie<Integer> newTrie = trie.find(c);
                if (newTrie == null) {
                    iterCodePoints.rewind();
                    break;
                }
                trie = newTrie;
            }
            return trie;
        }
        
        private @Nonnull IntPredicate determineCharacterClass(int c) {
            for (IntPredicate predicate : characterClasses) {
                if (predicate.test(c)) {
                    return predicate;
                }
            }
            return otherChars; 
        }

        private void readCharacterClass(IntPredicate characterClass) {
            while (iterCodePoints.hasNext()) {
                int c = iterCodePoints.next();
                if (!characterClass.test(c)) {
                    iterCodePoints.rewind();
                    break;
                }
            }
        }
    }
}
