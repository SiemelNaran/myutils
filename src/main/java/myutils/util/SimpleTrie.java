package myutils.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class SimpleTrie<T> {
    private boolean word;
    private Map<T, SimpleTrie<T>> children = new TreeMap<>();

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
    
    public void visit(BiConsumer<List<T>, SimpleTrie<T>> consumer) {
        doVisit(new LinkedList<T>(), consumer);
    }
    
    private void doVisit(LinkedList<T> list, BiConsumer<List<T>, SimpleTrie<T>> consumer) {
        for (Map.Entry<T, SimpleTrie<T>> entry : children.entrySet()) {
            list.add(entry.getKey());
            consumer.accept(list, entry.getValue());
            doVisit(list, consumer);
            list.removeLast();
        }
    }
}
