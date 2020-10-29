package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sn.myutils.testutils.TestBase;
import org.sn.myutils.testutils.TestUtil;


public class MoreCollectorsTest extends TestBase {
    static class Person {
        private final int id;
        private final String firstName;
        private final String lastName;
        private final int ageInSeconds;

        Person(int id, String firstName, String lastName, int ageInSeconds) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.ageInSeconds = ageInSeconds;
        }

        public int getId() {
            return id;
        }

        public String getFirstName() {
            return firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public int getAgeInSeconds() {
            return ageInSeconds;
        }
    }

    private static List<Person> generateRandomList(int size, int maxAgeInSeconds) {
        List<Person> result = new ArrayList<>(size);
        Random random = new Random();
        for (int i = 1; i <= size; i++) {
            var person = new Person(i, "Firstname" + i, "Lastname" + i, random.nextInt(maxAgeInSeconds) + 1);
            result.add(person);
        }
        return result;
    }

    /**
     * Verify that MoreCollectors.maxAndMinBy returns the same thing as Collectors.minBy and Collectors.maxBy.
     * If two Person's have the same age in seconds, return the first person.
     *
     * <p>The sequential way is sometimes faster than the parallel way.  No idea why!
     */
    @Test
    void testMinAndMaxBy() {
        List<Person> listPerson = generateRandomList(1_000_000, Math.toIntExact(Duration.ofMinutes(1).toSeconds()));
        Person personWithMinAge = listPerson.stream().min(Comparator.comparing(Person::getAgeInSeconds)).get();
        Person personWithMaxAge = listPerson.stream().max(Comparator.comparing(Person::getAgeInSeconds)).get();
        var result = listPerson.parallelStream().collect(MoreCollectors.minAndMaxBy(Comparator.comparing(Person::getAgeInSeconds))).get();
        assertSame(personWithMinAge, result.getMinValue());
        assertSame(personWithMaxAge, result.getMaxValue());
    }

    @Test
    void testMinAndMaxByParallel() {
        List<Person> listPerson = generateRandomList(1_000_000, Math.toIntExact(Duration.ofDays(365 * 50).toSeconds()));
        Person personWithMinAge = listPerson.stream().min(Comparator.comparing(Person::getAgeInSeconds)).get();
        Person personWithMaxAge = listPerson.stream().max(Comparator.comparing(Person::getAgeInSeconds)).get();
        var result = listPerson.parallelStream().collect(MoreCollectors.minAndMaxBy(Comparator.comparing(Person::getAgeInSeconds))).get();
        assertEquals(personWithMinAge.getAgeInSeconds(), result.getMinValue().getAgeInSeconds());
        assertEquals(personWithMaxAge.getAgeInSeconds(), result.getMaxValue().getAgeInSeconds());
    }

    @Test
    void testMinAndMaxByEmpty() {
        List<Person> listPerson = Collections.emptyList();
        var result = listPerson.stream().collect(MoreCollectors.minAndMaxBy(Comparator.comparing(Person::getAgeInSeconds)));
        assertTrue(result.isEmpty());
    }

    /**
     * Typical time with one million elements and numElements=1000 is
     * 59ms for maxBy/minBy and 848ms for the dumb method.
     *
     * <p>We test using both sequential and parallel streams (the latter exercises the combiner).
     * The parallel way is routinely 2.5 times slower!
     */
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = { "maxBy","minBy" })
    void testMaxBy(String method) {
        List<Person> listPerson = generateRandomList(1_000_000, Math.toIntExact(Duration.ofDays(30).toSeconds()));
        final int numElements = 1000;
        Instant now;

        // Use our maxBy function, which uses PriorityQueue
        now = Instant.now();
        PriorityQueue<Person> top;
        if (method.equals("maxBy")) {
            top = listPerson.stream().collect(MoreCollectors.maxBy(Comparator.comparing(Person::getAgeInSeconds), numElements));
        } else {
            top = listPerson.stream().collect(MoreCollectors.minBy(Comparator.comparing(Person::getAgeInSeconds), numElements));
        }
        System.out.printf("smartTime=%dms%n", Duration.between(now, Instant.now()).toMillis());
        assertEquals(numElements, top.size());

        // Use our maxBy function, which uses PriorityQueue
        now = Instant.now();
        PriorityQueue<Person> topParallel;
        if (method.equals("maxBy")) {
            topParallel = listPerson.parallelStream().collect(MoreCollectors.maxBy(Comparator.comparing(Person::getAgeInSeconds), numElements));
        } else {
            topParallel = listPerson.parallelStream().collect(MoreCollectors.minBy(Comparator.comparing(Person::getAgeInSeconds), numElements));
        }
        System.out.printf("smartTimeParallel=%dms%n", Duration.between(now, Instant.now()).toMillis());
        assertEquals(numElements, topParallel.size());

        // Use the dumb method, which is to sort the array and keep the last 1000 elements
        now = Instant.now();
        listPerson.sort(Comparator.comparing(Person::getAgeInSeconds));
        if (method.equals("maxBy")) {
            listPerson.subList(0, listPerson.size() - numElements).clear();
        } else {
            listPerson.subList(numElements, listPerson.size()).clear();
        }
        System.out.printf("dumbTime=%dms%n", Duration.between(now, Instant.now()).toMillis());
        assertEquals(numElements, listPerson.size());

        // compare the values in both arrays
        int[] smart = top.stream().mapToInt(Person::getAgeInSeconds).sorted().toArray();
        int[] dumb = listPerson.stream().mapToInt(Person::getAgeInSeconds).toArray();
        assertArrayEquals(smart, dumb);
    }
}
