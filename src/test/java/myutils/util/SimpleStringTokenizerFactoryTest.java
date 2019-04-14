package myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntPredicate;

import org.junit.jupiter.api.Test;

import myutils.util.SimpleStringTokenizerFactory.QuoteStrategy;
import myutils.util.SimpleStringTokenizerFactory.Token;


public class SimpleStringTokenizerFactoryTest {
    @Test
    public void testSpecialCharacter() {
        String str = "ê";
        assertEquals(1, str.length());
        str.codePoints().forEach(intVal -> System.out.print(intVal + ","));
        System.out.println();
        str.chars().forEach(intVal -> System.out.print(intVal + ","));
        System.out.println();
        assertTrue(Character.isLetter(str.codePointAt(0)));
    }
    
    @Test
    void testNoMoreElements() {
        Iterator<Token> tokenizer = createSimpleStringTokenizer("  \t\n\r    ");
        
        assertFalse(tokenizer.hasNext());
        try {
            tokenizer.next();
            fail();
        } catch (NoSuchElementException ignored) {
        }
    }
    
    @Test
    void testYesMoreElements() {
        Iterator<Token> tokenizer = createSimpleStringTokenizer("  \t\n\r R   ");
        assertTrue(tokenizer.hasNext());
        Token token = tokenizer.next();
        assertEquals("R", token.getText());
        assertEquals(6, token.getStart());
        
        assertFalse(tokenizer.hasNext());
        try {
            tokenizer.next();
            fail();
        } catch (NoSuchElementException ignored) {
        }
    }
    
    @Test
    void testTokenStart() {
        Iterator<Token> tokenizer = createSimpleStringTokenizer("a");
        assertTrue(tokenizer.hasNext());
        Token token = tokenizer.next();
        assertEquals("a", token.getText());
        assertEquals(0, token.getStart());
        assertFalse(tokenizer.hasNext());
        
        tokenizer = createSimpleStringTokenizer(" a");
        assertTrue(tokenizer.hasNext());
        token = tokenizer.next();
        assertEquals("a", token.getText());
        assertEquals(1, token.getStart());
        assertFalse(tokenizer.hasNext());
    }
    
    @Test
    void test() {
        Token token;
        Iterator<Token> tokenizer = createSimpleStringTokenizer("  hello(world++2.9**3) +\t\n  9  ");
                                                              // 012    7     3    8 0  3    6 8
        
        token = tokenizer.next();
        assertEquals("hello", token.getText());
        assertEquals(2, token.getStart());
        
        token = tokenizer.next();
        assertEquals("(", token.getText());
        assertEquals(7, token.getStart());
        
        token = tokenizer.next();
        assertEquals("world", token.getText());
        assertEquals(8, token.getStart());
        
        token = tokenizer.next();
        assertEquals("+", token.getText());
        assertEquals(13, token.getStart());
        
        token = tokenizer.next();
        assertEquals("+", token.getText());
        assertEquals(14, token.getStart());
        
        token = tokenizer.next();
        assertEquals("2.9", token.getText());
        assertEquals(15, token.getStart());
        
        token = tokenizer.next();
        assertEquals("**", token.getText());
        assertEquals(18, token.getStart());
        
        token = tokenizer.next();
        assertEquals("3", token.getText());
        assertEquals(20, token.getStart());
        
        token = tokenizer.next();
        assertEquals(")", token.getText());
        assertEquals(21, token.getStart());
        
        token = tokenizer.next();
        assertEquals("+", token.getText());
        assertEquals(23, token.getStart());
        
        token = tokenizer.next();
        assertEquals("9", token.getText());
        assertEquals(28, token.getStart());
        
        assertFalse(tokenizer.hasNext());
    }

    @Test
    void testSingleQuotedStrings() {
        Iterator<Token> tokenizer = createSimpleStringTokenizer("  'hello \\'abc\\' world' stuff ");

        Token token = tokenizer.next();
        assertEquals(19, token.getText().length());
        assertEquals("'hello 'abc' world'", token.getText());
        assertEquals(3, token.getStart());
        
        token = tokenizer.next();
        assertEquals("stuff", token.getText());
        assertFalse(tokenizer.hasNext());
    }
    
    @Test
    void testDoubleQuotedStrings() {
        Iterator<Token> tokenizer = createSimpleStringTokenizer("  \"hello \\\"abc\\\" world\" stuff ");
        
        Token token = tokenizer.next();
        assertEquals(19, token.getText().length());
        assertEquals("\"hello \"abc\" world\"", token.getText());
        assertEquals(3, token.getStart());
        
        token = tokenizer.next();
        assertEquals("stuff", token.getText());
        assertFalse(tokenizer.hasNext());
    }
    
    private static final IntPredicate SKIP_CHARACTERS
        = codePoint -> Character.isWhitespace(codePoint);
    
    private static final List<String> SYMBOLS
        = Arrays.asList("(", ")", "**", "+", "*");
    
    private static final IntPredicate LITERAL_CLASS
        = codePoint -> Character.isLetterOrDigit(codePoint) || codePoint == '.';

    private Iterator<Token> createSimpleStringTokenizer(String expression) {
        return new SimpleStringTokenizerFactory(SKIP_CHARACTERS,
                                         new QuoteStrategy(true, true),
                                         SYMBOLS,
                                         Collections.singletonList(LITERAL_CLASS))
                .tokenizer(expression);
    }

}
