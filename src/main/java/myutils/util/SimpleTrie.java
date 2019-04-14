package myutils.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SimpleTrie<T> {
    private boolean word;
    private Map<T, SimpleTrie<T>> children = new HashMap<>();

    public void add(Stream<T> codePoints) {
        doAdd(this, codePoints);
    }
    
    private static <T> void doAdd(@Nonnull SimpleTrie<T> trie, Stream<T> codePoints) {
        for (Iterator<T> iter = codePoints.iterator(); iter.hasNext(); ) {
            T codePoint = iter.next();
            trie = trie.children.computeIfAbsent(codePoint, ignored -> new SimpleTrie<T>());
        }
        trie.setWord(true);
    }
    
    private void setWord(boolean word) {
        this.word = word;
    }
    
    public @Nullable SimpleTrie<T> find(T val) {
        return children.get(val);
    }

    public boolean isWord() {
        return word;
    }
}
