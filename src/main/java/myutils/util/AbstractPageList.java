package myutils.util;

import static java.lang.Math.min;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * {@inheritDoc}
 * 
 * <p>This class implements RandomAccess even though looking up an element is O(lg(N)).
 * 
 * <p>This class implements java.io.Serializable.
 * 
 * <p>The spliterator splits by pages. If a page has too many elements in it, the page itself
 * does not get split.
 */
@NotThreadSafe
public abstract class AbstractPageList<E> extends AbstractList<E> implements PageList<E>, RandomAccess, Serializable {
    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_PREFERRED_MAX_PAGE_SIZE = 50;
    private static final int DEFAULT_MAX_PAGE_SIZE = 100;

    protected final int preferredMaxPageSize;
    protected final int maxPageSize;
    private transient int modCount = 0;
    private int size;
    private final ArrayList<Page<E>> pages = new ArrayList<>(); // sorted by Page.startIndex
    
    AbstractPageList() {
        this(Collections.emptyList(), DEFAULT_PREFERRED_MAX_PAGE_SIZE, DEFAULT_MAX_PAGE_SIZE);
    }

    AbstractPageList(Collection<E> elements) {
        this(elements, DEFAULT_PREFERRED_MAX_PAGE_SIZE, DEFAULT_MAX_PAGE_SIZE);
    }

    AbstractPageList(Collection<E> elements, int preferredMaxPageSize, int maxPageSize) {
        if (!(preferredMaxPageSize < maxPageSize)) {
            throw new IllegalArgumentException("preferredMaxPageSize(" + preferredMaxPageSize + ") should be < maxPageSize(" + maxPageSize + ")");
        }
        if (preferredMaxPageSize < 3) {
            throw new IllegalArgumentException("preferredMaxPageSize(" + preferredMaxPageSize + ") should be >= 3");
        }
        this.preferredMaxPageSize = preferredMaxPageSize;
        this.maxPageSize = maxPageSize;
        addAll(elements);
        modCount = 0;
    }

    @Override
    public int size() {
        return size;
    }
    
    @Override
    public E remove(int index) {
        rangeCheckExcludeEnd(index);
        int pageIndex = findPage(index);
        Page<E> page = pages.get(pageIndex);
        int elementInPageIndex = index - page.startIndex;
        ListIterator<E> elementIter = page.list.listIterator(elementInPageIndex);
        E element = elementIter.next();
        doRemove(pageIndex, page, elementIter);
        return element;
    }
    
    // CHECKSTYLE:OFF SummaryJavadoc
    /**
     * @return true if page was removed
     */
    // CHECKSTYLE:ON
    private boolean doRemove(int pageIndex, Page<E> page, ListIterator<E> elementIter) {
        elementIter.remove();
        modCount++;
        final boolean pageRemoved;
        if (page.list.size() == 0) {
            pages.remove(pageIndex);
            updateIndices(pageIndex, -1);
            pageRemoved = true;
        } else {
            updateIndices(pageIndex + 1, -1);
            pageRemoved = false;
        }
        size--;
        return pageRemoved;
    }
    
    @Override
    public void add(int index, E element) {
        rangeCheckAllowEnd(index);
        if (index == size) {
            Page<E> lastPage;
            if (size == 0 || (lastPage = pages.get(pages.size() - 1)).list.size() >= preferredMaxPageSize) {
                Page<E> newPage = this.createNewEmptyPage(size);
                newPage.list.add(element);
                pages.add(newPage);
            } else {
                lastPage.list.add(element);
            }
            modCount++;
            size++;
        } else {
            int pageIndex = findPage(index);
            Page<E> page = pages.get(pageIndex);
            int elementInPageIndex = index - page.startIndex;
            doAdd(pageIndex, page, page.list.listIterator(elementInPageIndex), element);
        }
    }
    
    private enum RecalcIterAfterAdd {
        NONE,
        LAST_ELEMENT,
        SECOND_ELEMENT_NEXT_PAGE
    }
    
    private RecalcIterAfterAdd doAdd(int pageIndex, Page<E> page, ListIterator<E> elementIter, E element) {
        final RecalcIterAfterAdd recalcIter;
        if (elementIter.hasNext()) {
            if (page.list.size() < maxPageSize) {
                elementIter.add(element);
                recalcIter = RecalcIterAfterAdd.NONE;
            } else {
                int elementInPageIndex = elementIter.nextIndex();
                Page<E> remainderPage = createNewPageByCopyingSublist(page.startIndex + elementInPageIndex + 1,
                                                                      page.list.subList(elementInPageIndex, page.list.size()));
                page.list.subList(elementInPageIndex, page.list.size()).clear();
                pageIndex++;
                pages.add(pageIndex, remainderPage);
                page.list.add(element);
                recalcIter = RecalcIterAfterAdd.LAST_ELEMENT;
            }
        } else {
            if (page.list.size() < preferredMaxPageSize) {
                elementIter.add(element);
                recalcIter = RecalcIterAfterAdd.NONE;
            } else {
                Page<E> newPage = createNewEmptyPage(page.startIndex + page.list.size());
                pageIndex++;
                pages.add(pageIndex, newPage);
                newPage.list.add(element);
                recalcIter = RecalcIterAfterAdd.SECOND_ELEMENT_NEXT_PAGE;
            }
        }
        modCount++;
        updateIndices(pageIndex + 1, 1);
        size++;
        return recalcIter;
    }
    
    @Override
    public boolean addAll(Collection<? extends E> elements) {
        return addAll(size, elements);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> elements) {
        rangeCheckAllowEnd(index);
        
        int numElementsToCopy = elements.size();
        if (numElementsToCopy == 0) {
            return false;
        }
        
        if (index == size) {
            addAllAtEnd(numElementsToCopy, elements);
        } else {
            addAllInMiddle(index, numElementsToCopy, elements);
        }
        
        modCount++;
        size += elements.size();
        return true;
    }
    
    private void addAllAtEnd(int numElementsToCopy, Collection<? extends E> elements) {
        Page<E> page;
        if (size == 0) {
            page = createNewEmptyPage(0);
            pages.add(page);
        } else {
            page = pages.get(pages.size() - 1);
        }
            
        Iterator<? extends E> elementsIter = elements.iterator();

        int numPagesToCreate = divideUp(numElementsToCopy, preferredMaxPageSize);
        List<Page<E>> newPages = new ArrayList<>(numPagesToCreate);
        int currentStart = page.startIndex;
    
        while (elementsIter.hasNext()) {
            if (page.list.size() >= preferredMaxPageSize) {
                currentStart += page.list.size();
                page = createNewEmptyPage(currentStart);
                newPages.add(page);
            }
            page.list.add(elementsIter.next());
        }
        
        pages.addAll(pages.size(), newPages);
    }
    
    private void addAllInMiddle(int index, int numElementsToCopy, Collection<? extends E> elements) {
        final int firstPageToUpdateIndices;

        final int originalPageIndex = findPage(index);
        Page<E> page = pages.get(originalPageIndex);
        int elementInPageIndex = index - page.startIndex;
        
        if (numElementsToCopy <= maxPageSize - page.list.size()) {
            // we are inserting a few elements and they can all be added to the current page
            firstPageToUpdateIndices = originalPageIndex + 1;
            page.list.addAll(elementInPageIndex, elements);
        } else {
            Iterator<? extends E> elementsIter = elements.iterator();
            int currentStart = page.startIndex;

            // we are inserting many elements, and need to create one or more pages 
            
            // if inserting the new elements in the middle of page, then
            //     copy all elements from preferredMaxPageSize to the end of the list to a remainder page
            // otherwise, there is no remainder page
            final Page<E> remainderPage;
            if (elementInPageIndex > 0) {
                remainderPage = createNewPageByCopyingSublist(page.startIndex + elementInPageIndex,
                                                              page.list.subList(elementInPageIndex, page.list.size()));
                page.list.subList(elementInPageIndex, page.list.size()).clear();
                
                // copy elements to bring the size of the page to preferredMaxPageSize
                while (elementsIter.hasNext() && page.list.size() < preferredMaxPageSize) {
                    page.list.add(elementsIter.next());
                    numElementsToCopy--;
                }
            } else {
                remainderPage = null;
                currentStart -= preferredMaxPageSize;
            }
            
            int numPagesToCreate = divideUp(numElementsToCopy, preferredMaxPageSize);
            List<Page<E>> newPages = new ArrayList<>(numPagesToCreate + 1); // +1 for remainder page
        
            while (elementsIter.hasNext()) {
                if (page.list.size() >= preferredMaxPageSize) {
                    currentStart += page.list.size();
                    page = createNewEmptyPage(currentStart);
                    newPages.add(page);
                }
                page.list.add(elementsIter.next());
            }
            
            final int indexWhereToInsertPages;
            
            // add remainder page to newPages
            if (remainderPage != null) {
                newPages.add(remainderPage);
                indexWhereToInsertPages = originalPageIndex + 1;
            } else {
                indexWhereToInsertPages = originalPageIndex;
            }
            
            // add all the new pages to this.pages
            pages.addAll(indexWhereToInsertPages, newPages);
            firstPageToUpdateIndices = originalPageIndex + newPages.size();
        }
            
        updateIndices(firstPageToUpdateIndices, elements.size());
    }
    
    private abstract class SpliceHandler {
        void addPageByMoveElements(int startIndex, Page<E> source, int start, int end) {
            addPageByCopyElements(startIndex, source, start, end);
            source.list.subList(start, end).clear();
        }
        
        abstract void shallowCopyPage(int newStartIndex, Page<E> page);

        abstract void addPageByCopyElements(int startIndex, Page<E> source, int start, int end);

        abstract void onComplete(int size);
    }
    
    @Override
    public AbstractPageList<E> splice(int startInclusive, int endExclusive) {
        final AbstractPageList<E> output = createNewPageList();
        
        SpliceHandler spliceHandler = new SpliceHandler() {
            @Override
            public void shallowCopyPage(int newStartIndex, Page<E> page) {
                page.startIndex = newStartIndex;
                output.pages.add(page);
            }
            
            @Override
            public void addPageByCopyElements(int startIndex, Page<E> source, int start, int end) {
                Page<E> newPage = createNewPageByCopyingSublist(startIndex, source.list.subList(start, end));
                output.pages.add(newPage);
            }

            @Override
            public void onComplete(int size) {
                output.size = size;                
            }
        };
        
        doSplice(startInclusive, endExclusive, spliceHandler);
        output.size = endExclusive - startInclusive;
        
        return output;
    }
    
    @Override
    protected void removeRange(int startInclusive, int endExclusive) {
        SpliceHandler spliceHandler = new SpliceHandler() {
            @Override
            public void shallowCopyPage(int newStartIndex, Page<E> page) {
            }
            
            @Override
            public void addPageByCopyElements(int startIndex, Page<E> source, int start, int end) {
            }

            @Override
            public void onComplete(int size) {
            }
        };
        
        doSplice(startInclusive, endExclusive, spliceHandler);
    }
    
    private void doSplice(int startInclusive, int endExclusive, SpliceHandler spliceHandler) {
        if (startInclusive > endExclusive) {
            throw new IllegalArgumentException("startInclusive(" + startInclusive + ") should be less than endExclusive(" + endExclusive + ")");
        }
        rangeCheckAllowEnd(startInclusive);
        rangeCheckAllowEnd(endExclusive);
        
        if (startInclusive == endExclusive) {
            return;
        }
        
        int firstPageIndex = findPage(startInclusive);
        int lastPageIndex = findPage(endExclusive - 1);
        int pageIndex = firstPageIndex;
        ListIterator<Page<E>> pageIter = pages.listIterator(pageIndex);
        Page<E> page = pageIter.next(); // will never throw NoSuchElementException because of rangeCheck above
        final int removeFrom;
        {
            int startInPage = startInclusive - page.startIndex;
            int endInPage = min(page.list.size(), endExclusive - startInclusive);
            if (startInPage == 0 && endInPage == page.list.size()) {
                // we are splicing the entire first page, so do a shallow copy of page,
                // and modify page.startIndex because this page will be removed at the end
                spliceHandler.shallowCopyPage(0, page);
                removeFrom = firstPageIndex;
            } else {
                // we are splicing some elements from the first page (maybe from the first element)
                spliceHandler.addPageByMoveElements(0, page, startInPage, endInPage);
                removeFrom = firstPageIndex + 1;
            }
            page = pageIter.next();
            pageIndex++;
        }
        // move entire middle pages from this to output, so do a shallow copy of each page.list
        for ( ; pageIndex < lastPageIndex; pageIndex++) {
            spliceHandler.shallowCopyPage(page.startIndex - startInclusive, page);
            page = pageIter.next();
        }
        final int removeTo;
        if (lastPageIndex > firstPageIndex) {
            // this branch is called if we are splicing elements from more than one page
            int lastSize = endExclusive - page.startIndex;
            if (lastSize == page.list.size()) {
                // we are splicing the entire last page, so do a shallow copy of page.list
                spliceHandler.shallowCopyPage(page.startIndex - startInclusive, page);
                removeTo = lastPageIndex + 1;
            } else {
                // we are splicing some elements from the last page (starting from the first element to before the last element)
                spliceHandler.addPageByMoveElements(page.startIndex - startInclusive, page, 0, lastSize);
                page.startIndex += lastSize;
                removeTo = lastPageIndex;
            }
        } else {
            removeTo = firstPageIndex + 1;
        }
        
        modCount++;
        
        // decrease the size of this, and increase the size of output from 0 to the number of elements copied
        this.size -= endExclusive - startInclusive;
        int outputSize = endExclusive - startInclusive;
        spliceHandler.onComplete(outputSize);
        
        // remove pages from the appropriate pages from this, and update the indices of this
        pages.subList(removeFrom, removeTo).clear();;
        updateIndices(removeFrom, -outputSize);
    }
    
    @Override
    public E set(int index, E element) {
        rangeCheckExcludeEnd(index);
        int pageIndex = findPage(index);
        Page<E> page = pages.get(pageIndex);
        int elementInPageIndex = index - page.startIndex;
        modCount++;
        return page.list.set(elementInPageIndex, element);
    }
    
    /**
     * {@inheritDoc}
     * 
     * <p>Get an element from the list.
     * Running time O(lg(N) + ?).
     * Prefer iterators when iterating over the list.
     */
    @Override
    public E get(int index) {
        rangeCheckExcludeEnd(index);
        int pageIndex = findPage(index);
        Page<E> page = pages.get(pageIndex);
        int elementInPageIndex = index - page.startIndex;
        return page.list.get(elementInPageIndex);
    }

    @Override
    public Iterator<E> iterator() {
        rangeCheckAllowEnd(0);
        return new Iter(0, 0);
    }
    
    @Override
    public ListIterator<E> listIterator(int index) {
        rangeCheckAllowEnd(index);
        int pageIndex = findPage(index);
        Page<E> page = pages.get(pageIndex);
        int elementInPageIndex = index - page.startIndex;
        return new ListIter(pageIndex, elementInPageIndex);
    }
    
    @Override
    public Spliterator<E> spliterator() {
        int averagePageSize = pages.size() > 0 ? size / pages.size() : 1; 
        return new PageListSpliterator<E>(modCount, averagePageSize, pages.spliterator());
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        int delta = 0;
        for (Page<E> page: pages) {
            page.startIndex -= delta;
            int originalSize = page.list.size();
            boolean changes = page.list.removeIf(filter);
            if (changes) {
                int newSize = page.list.size();
                delta += originalSize - newSize;
            }
        }
        if (delta == 0) {
            return false;
        }
        size -= delta;
        modCount++;
        return true;
    }
    
    @Override
    public void forEach(Consumer<? super E> operator) {
        for (Page<E> page : pages) {
            page.list.forEach(operator);
        }
        modCount++;
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {
        for (Page<E> page: pages) {
            page.list.replaceAll(operator);
        }
        modCount++;
    }

    /**
     * This implementation uses the default sort algorithm,
     * which copies the elements to a temp array, sorts the array,
     * then copies the elements in the temp array to this.
     */
    @Override
    public void sort(Comparator<? super E> comparator) {
        super.sort(comparator);
    }
    
    
    // helpers
    
    ArrayList<Page<E>> getPages() {
        return pages;
    }
    
    private void rangeCheckAllowEnd(int index) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

    private void rangeCheckExcludeEnd(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

    private int findPage(int index) {
        int pageIndex = MoreCollections.binarySearchInt(pages, 0, pages.size(), Page::getStartIndex, index);
        if (pageIndex < 0) {
            pageIndex = -pageIndex - 2;
        }
        return pageIndex;
    }
    
    private void updateIndices(int startPageIndex, int delta) {
        for (int i = startPageIndex; i < pages.size(); i++) {
            pages.get(i).startIndex += delta;
        }
    }
    
    private static int divideUp(int numerator, int denominator) {
        int result = numerator / denominator;
        int remainder = numerator % denominator;
        if (remainder != 0) {
            result++;
        }
        return result;
    }
    
    protected static class Page<E> implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private int startIndex;
        private final List<E> list;
        
        Page(int startIndex, List<E> list) {
            this.startIndex = startIndex;
            this.list = list;
        }
        
        int getStartIndex() {
            return startIndex;
        }
        
        List<E> getList() {
            return list;
        }
        
        @Override
        public String toString() {
            return startIndex + list.toString();
        }
    }
    
    protected abstract Page<E> createNewEmptyPage(int startIndex); 

    protected abstract Page<E> createNewPageByCopyingSublist(int startIndex, List<E> subListToCopy);
    
    protected abstract AbstractPageList<E> createNewPageList();
    
    private enum IterDirection {
        FORWARD,
        BACKWARD
    }
    
    private class Iter implements Iterator<E> {
        int expectedModCount = AbstractPageList.this.modCount;
        int pageIndex;
        ListIterator<E> elementIter;
        IterDirection direction;

        Iter(int pageIndex, int elementInPageIndex) {
            this.pageIndex = pageIndex;
            this.elementIter = getIteratorOfPage(elementInPageIndex);
        }
        
        @Override
        public final boolean hasNext() {
            checkForComodification();
            return pageIndex < pages.size() - 1 || elementIter.hasNext();
        }

        @Override
        public final E next() {
            checkForComodification();
            try {
                direction = IterDirection.FORWARD;
                if (!elementIter.hasNext()) {
                    elementIter = getIteratorOfPage(1, direction);
                }
                return elementIter.next();
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }
        

        @Override
        public final void remove() {
            checkForComodification();
            Page<E> page;
            try {
                page = pages.get(pageIndex);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalStateException();
            }
            boolean pageRemoved = doRemove(pageIndex, page, elementIter); // throws IllegalStateException if direction is null
            if (pageRemoved) {
                switch (direction) {
                    case FORWARD:
                        elementIter = getIteratorOfPage(0, direction);
                        break;
                    
                    case BACKWARD:
                        if (size > 0) {
                            if (pageIndex > 0) {
                                elementIter = getIteratorOfPage(-1, direction);
                            } else {
                                elementIter = getIteratorOfPage(0, IterDirection.FORWARD);
                            }
                        } else {
                            elementIter = getIteratorOfPage(0);
                        }
                        break;
                        
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            direction = null;
            this.expectedModCount = AbstractPageList.this.modCount;
        }

        private final ListIterator<E> getIteratorOfPage(int elementInPageIndex) {
            Page<E> page;
            try {
                page = pages.get(pageIndex);
            } catch (IndexOutOfBoundsException e) {
                return Collections.<E>emptyList().listIterator();
            }
            return page.list.listIterator(elementInPageIndex);
        }
        
        final ListIterator<E> getIteratorOfPage(int pageDelta, IterDirection direction) {
            int newPageIndex = pageIndex + pageDelta;
            Page<E> page = pages.get(newPageIndex); // never throws because newPageIndex always in [0, pages.size)
            pageIndex = newPageIndex;
            switch (direction) {
                case FORWARD:
                    return page.list.listIterator();
                case BACKWARD:
                    return page.list.listIterator(page.list.size());
                default:
                    throw new UnsupportedOperationException();
            }
        }
        
        final void checkForComodification() {
            if (AbstractPageList.this.modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }
    }
    
    private final class ListIter extends Iter implements ListIterator<E> {
        ListIter(int pageIndex, int elementInPageIndex) {
            super(pageIndex, elementInPageIndex);
        }

        @Override
        public boolean hasPrevious() {
            checkForComodification();
            return pageIndex > 0 || elementIter.hasPrevious();
        }

        @Override
        public E previous() {
            checkForComodification();
            try {
                direction = IterDirection.BACKWARD;
                if (!elementIter.hasPrevious()) {
                    elementIter = getIteratorOfPage(-1, direction);
                }
                return elementIter.previous();
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        /**
         * {@inheritDoc}
         * 
         * @throws ConcurrentModificationException if array changed outside this iterator, whereas ArrayList's nextIndex does not throw.
         */
        @Override
        public int nextIndex() {
            checkForComodification();
            if (pageIndex == pages.size()) {
                return size;
            } else {
                Page<E> page = pages.get(pageIndex);
                return page.startIndex + elementIter.nextIndex();
            }
        }

        /**
         * {@inheritDoc}
         * 
         * @throws ConcurrentModificationException if array changed outside this iterator, whereas ArrayList's nextIndex does not throw.
         */
        @Override
        public int previousIndex() {
            return nextIndex() - 1;
        }

        @Override
        public void set(E element) {
            checkForComodification();
            elementIter.set(element);
            expectedModCount = ++AbstractPageList.this.modCount;
        }

        @Override
        public void add(E element) {
            checkForComodification();
            Page<E> page;
            try {
                page = pages.get(pageIndex);
            } catch (IndexOutOfBoundsException e) {
                AbstractPageList.this.add(0, element);
                pageIndex = 0;
                elementIter = pages.get(0).list.listIterator(1);
                expectedModCount = AbstractPageList.this.modCount;
                return;
            }
            RecalcIterAfterAdd recalcIter = doAdd(pageIndex, page, elementIter, element);
            switch (recalcIter) {
                case LAST_ELEMENT:
                    elementIter = page.list.listIterator(page.list.size());
                    break;
                    
                case SECOND_ELEMENT_NEXT_PAGE:
                    pageIndex++;
                    page = pages.get(pageIndex);
                    elementIter = page.list.listIterator(1);
                    break;
                    
                case NONE:
                    break;
                    
                default:
                    throw new UnsupportedOperationException();
            }
            expectedModCount = AbstractPageList.this.modCount;
        }
    }

    private static final class PageListSpliterator<E> implements Spliterator<E> {
        private final int expectedModCount;
        private final int averagePageSize;
        private final Spliterator<Page<E>> pageSpliterator;
        private Spliterator<E> innerSpliterator;

        private PageListSpliterator(int modCount, int averagePageSize, Spliterator<Page<E>> pageSpliterator) {
            this.expectedModCount = modCount;
            this.averagePageSize = averagePageSize;
            this.pageSpliterator = pageSpliterator;
            pageSpliterator.tryAdvance(page -> innerSpliterator = page.list.spliterator());
        }

        @Override
        public PageListSpliterator<E> trySplit() {
            Spliterator<Page<E>> split = pageSpliterator.trySplit();
            if (split == null) {
                return null;
            }
            return new PageListSpliterator<E>(expectedModCount, averagePageSize, split);
        }

        @Override
        public boolean tryAdvance(Consumer<? super E> action) {
            if (innerSpliterator == null) {
                return false;
            }
            boolean done = innerSpliterator.tryAdvance(action);
            if (!done) {
                boolean hasNextPage = pageSpliterator.tryAdvance(page -> innerSpliterator = page.list.spliterator());
                if (hasNextPage) {
                    done = innerSpliterator.tryAdvance(action);
                } else {
                    innerSpliterator = null;
                }
            }
            return done;
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            if (innerSpliterator == null) {
                return;
            }
            do {
                innerSpliterator.forEachRemaining(action);
            } while (pageSpliterator.tryAdvance(page -> innerSpliterator = page.list.spliterator()));
            innerSpliterator = null;
        }

        @Override
        public long estimateSize() {
            return (pageSpliterator.estimateSize() + 1) * averagePageSize;
        }

        @Override
        public int characteristics() {
            return ORDERED;
        }        
    }
}
