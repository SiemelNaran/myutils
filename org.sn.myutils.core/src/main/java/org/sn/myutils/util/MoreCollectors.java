package org.sn.myutils.util;

import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.sn.myutils.annotations.NotNull;


public class MoreCollectors {
    /**
     * Given a stream, return both the minimum and maximum
     * If the stream is empty return an empty Optional.
     * Otherwise return an Optional of an an array of two elements,
     * where the first element is the minimum and the second is the maximum.
     */
    public static <T> Collector<T, MinMax<T>, Optional<MinMax<T>>>
    minAndMaxBy(Comparator<? super T> comparator) {
        return new Collector<>() {
            @Override
            public Supplier<MinMax<T>> supplier() {
                return MinMax::new;
            }

            @Override
            public BiConsumer<MinMax<T>, T> accumulator() {
                return (result, value) -> {
                    result.min = result.min == null || comparator.compare(value, result.min) < 0 ? value : result.min;
                    result.max = result.max == null || comparator.compare(value, result.max) > 0 ? value : result.max;
                };
            }

            @Override
            public BinaryOperator<MinMax<T>> combiner() {
                return (lhs, rhs) -> {
                    T min = comparator.compare(lhs.min, rhs.min) <= 0 ? lhs.min : rhs.min;
                    T max = comparator.compare(rhs.max, lhs.max) > 0 ? rhs.max : lhs.max;
                    return new MinMax<>(min, max);
                };
            }

            @Override
            public Function<MinMax<T>, Optional<MinMax<T>>> finisher() {
                return result -> {
                    if (result.min == null) {
                        return Optional.empty();
                    }
                    return Optional.of(result);
                };
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Set.of();
            }
        };
    }

    public static final class MinMax<T> {
        private T min;
        private T max;

        MinMax() {
        }

        MinMax(@NotNull T min, @NotNull T max) {
            this.min = min;
            this.max = max;
        }

        public T getMinValue() {
            return min;
        }

        public T getMaxValue() {
            return max;
        }
    }


    /**
     * Given a stream, return the N largest elements.
     * The result is a PriorityQueue with the head holding the smallest element.
     */
    public static <T> Collector<T, PriorityQueue<T>, PriorityQueue<T>>
    maxBy(Comparator<? super T> comparator, int count) {
        return new Collector<>() {
            @Override
            public Supplier<PriorityQueue<T>> supplier() {
                return () -> new PriorityQueue<T>(comparator);
            }

            @Override
            public BiConsumer<PriorityQueue<T>, T> accumulator() {
                return (result, value) -> {
                    if (result.size() < count) {
                        result.add(value);
                    } else if (comparator.compare(value, result.peek()) > 0) {
                        result.poll();
                        result.add(value);
                    }
                };
            }

            @Override
            public BinaryOperator<PriorityQueue<T>> combiner() {
                return (lhs, rhs) -> {
                    int size = lhs.size() + rhs.size();
                    while (size > count) {
                        if (comparator.compare(lhs.peek(), rhs.peek()) < 0) {
                            lhs.poll();
                        } else {
                            rhs.poll();
                        }
                        size--;
                    }
                    lhs.addAll(rhs);
                    return lhs;
                };
            }

            @Override
            public Function<PriorityQueue<T>, PriorityQueue<T>> finisher() {
                return Function.identity();
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Set.of(Characteristics.IDENTITY_FINISH);
            }
        };
    }

    /**
     * Given a stream, return the N smallest elements.
     * The result is a PriorityQueue with the head holding the largest element.
     */
    public static <T> Collector<T, PriorityQueue<T>, PriorityQueue<T>>
    minBy(Comparator<? super T> comparator, int count) {
        return new Collector<>() {
            @Override
            public Supplier<PriorityQueue<T>> supplier() {
                return () -> new PriorityQueue<T>(comparator.reversed());
            }

            @Override
            public BiConsumer<PriorityQueue<T>, T> accumulator() {
                return (result, value) -> {
                    if (result.size() < count) {
                        result.add(value);
                    } else if (comparator.compare(value, result.peek()) < 0) {
                        result.poll();
                        result.add(value);
                    }
                };
            }

            @Override
            public BinaryOperator<PriorityQueue<T>> combiner() {
                return (lhs, rhs) -> {
                    int size = lhs.size() + rhs.size();
                    while (size > count) {
                        if (comparator.compare(lhs.peek(), rhs.peek()) > 0) {
                            lhs.poll();
                        } else {
                            rhs.poll();
                        }
                        size--;
                    }
                    lhs.addAll(rhs);
                    return lhs;
                };
            }

            @Override
            public Function<PriorityQueue<T>, PriorityQueue<T>> finisher() {
                return Function.identity();
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Set.of(Characteristics.IDENTITY_FINISH);
            }
        };
    }
}
