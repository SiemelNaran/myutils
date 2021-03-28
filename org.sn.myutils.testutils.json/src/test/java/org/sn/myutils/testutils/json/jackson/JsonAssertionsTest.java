package org.sn.myutils.testutils.json.jackson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.sn.myutils.testutils.TestUtil.assertException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.json.JsonComparisonAssertionError;


public class JsonAssertionsTest {
    /**
     * Test two JSON arrays are equal, covering the happy case.
     */
    @Test
    public void testAssertJsonEquals1() throws IOException {
        var jsonComparisonBuilder =
                JsonAssertions.JsonComparisonBuilder.newBuilder()
                                                    .addPathToIdField("", "recordId")
                                                    .addPathToIdField("[].child.someArray", "id")
                                                    .addPathForWhichDontCompareValue("[].intOrLong")
                                                    .addPathForWhichDontCompareValue("[].floatOrDouble")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("[].deleted")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("[].child.someArray[].booleanFlag");

        JsonNode json1 = loadJsonFromFile("testAssertJsonEquals1-First.json");
        JsonNode json2 = loadJsonFromFile("testAssertJsonEquals1-Second.json");

        JsonAssertions.assertJsonEquals(json1, json2, jsonComparisonBuilder);
        JsonAssertions.assertJsonEquals(json2, json1, jsonComparisonBuilder);
    }

    /**
     * Test two JSON arrays equal by order, as no id field is specified.
     */
    @Test
    public void testAssertJsonEquals2() throws IOException {
        var jsonComparisonBuilder =
                JsonAssertions.JsonComparisonBuilder.newBuilder()
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("someArray[].booleanFlag");

        JsonNode json1 = loadJsonFromFile("testAssertJsonEquals2-First.json");
        JsonNode json2 = loadJsonFromFile("testAssertJsonEquals2-Second.json");

        JsonAssertions.assertJsonEquals(json1, json2, jsonComparisonBuilder);
        JsonAssertions.assertJsonEquals(json2, json1, jsonComparisonBuilder);
    }

    /**
     * Test most cases of two JSON objects not being equal.
     */
    @Test
    public void testAssertJsonNotEquals1() throws IOException {
        var jsonComparisonBuilder =
                JsonAssertions.JsonComparisonBuilder.newBuilder()
                                                    .addPathToIdField("", "recordId")
                                                    .addPathToIdField("[].child.someArray", "id")
                                                    .addPathForWhichDontCompareValue("[].mismatchedType")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("[].deleted")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("[].child.someArray[].booleanFlag");

        JsonNode json1 = loadJsonFromFile("testAssertJsonNotEquals1-First.json");
        JsonNode json2 = loadJsonFromFile("testAssertJsonNotEquals1-Second.json");

        assertException(
            () -> JsonAssertions.assertJsonEquals(json1, json2, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("[recordId=r1].firstName=First: Expected First but got FirstWrong",
                                                  "[recordId=r1].middleName=ExtraAttribute: Attribute in actual but not in expected",
                                                  "[recordId=r1].lastName=Something: Expected TextNode(Something) but got IntNode(999)",
                                                  "[recordId=r1].mismatchedType=1111: Expected type IntNode but got type TextNode",
                                                  "[recordId=r1].child.someArray: Expected JSON with 3 elements, but got JSON with 4 elements")));

        printSeparator();
        assertException(
            () -> JsonAssertions.assertJsonEquals(json2, json1, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("[recordId=r1].firstName=FirstWrong: Expected FirstWrong but got First",
                                                  "[recordId=r1].middleName=ExtraAttribute: Attribute in expected but not in actual",
                                                  "[recordId=r1].lastName=999: Expected IntNode(999) but got TextNode(Something)",
                                                  "[recordId=r1].mismatchedType=SomeString: Expected type TextNode but got type IntNode",
                                                  "[recordId=r1].child.someArray: Expected JSON with 4 elements, but got JSON with 3 elements",
                                                  "[recordId=r1].child.someArray[id=4]: Record not in actual")));
    }

    /**
     * Test comparing arrays in wrong order, and no id field is specified.
     */
    @Test
    public void testAssertJsonNotEquals2() throws IOException {
        var jsonComparisonBuilder =
                JsonAssertions.JsonComparisonBuilder.newBuilder()
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("someArray[].booleanFlag");

        JsonNode json1 = loadJsonFromFile("testAssertJsonNotEquals2-First.json");
        JsonNode json2 = loadJsonFromFile("testAssertJsonNotEquals2-Second.json");

        assertException(
            () -> JsonAssertions.assertJsonEquals(json1, json2, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("someArray[1].id=2: Expected 2 but got 3",
                                                  "someArray[1].name=two: Expected two but got three",
                                                  "someArray[2].id=3: Expected 3 but got 2",
                                                  "someArray[2].name=three: Expected three but got two")));

        printSeparator();
        assertException(
            () -> JsonAssertions.assertJsonEquals(json2, json1, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("someArray[1].id=3: Expected 3 but got 2",
                                                  "someArray[1].name=three: Expected three but got two",
                                                  "someArray[2].id=2: Expected 2 but got 3",
                                                  "someArray[2].name=two: Expected two but got three")));
    }

    /**
     * Test case when one JSON array has two occurrences of the same id value.
     */
    @Test
    public void testAssertJsonNotEquals3() throws IOException {
        var jsonComparisonBuilder =
                JsonAssertions.JsonComparisonBuilder.newBuilder()
                                                    .addPathToIdField("someArray", "id")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("someArray[].booleanFlag");

        JsonNode json1 = loadJsonFromFile("testAssertJsonNotEquals3-First.json");
        JsonNode json2 = loadJsonFromFile("testAssertJsonNotEquals3-Second.json");

        assertException(
            () -> JsonAssertions.assertJsonEquals(json1, json2, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("someArray: Expected JSON with 3 elements, but got JSON with 4 elements",
                                                  "someArray[id=2]: Too many records in actual",
                                                  "someArray[id=2].name=two: Expected two but got anotherTwo")));

        printSeparator();
        assertException(
            () -> JsonAssertions.assertJsonEquals(json2, json1, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("someArray: Expected JSON with 4 elements, but got JSON with 3 elements",
                                                  "someArray[id=2].name=anotherTwo: Expected anotherTwo but got two")));
    }


    /**
     * Test case when id field is missing.
     */
    @Test
    public void testAssertJsonNotEquals4() throws IOException {
        var jsonComparisonBuilder =
                JsonAssertions.JsonComparisonBuilder.newBuilder()
                                                    .addPathToIdField("someArray", "id")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("someArray[].booleanFlag");

        JsonNode json1 = loadJsonFromFile("testAssertJsonNotEquals4-First.json");
        JsonNode json2 = loadJsonFromFile("testAssertJsonNotEquals4-Second.json");

        assertException(
            () -> JsonAssertions.assertJsonEquals(json1, json2, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("someArray[id=2]: Record not in actual")));

        printSeparator();
        assertException(
            () -> JsonAssertions.assertJsonEquals(json2, json1, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("someArray[id=]: Record not in actual")));
    }

    /**
     * Test case when attribute missing in one JSON and it has default value.
     */
    @Test
    public void testAssertJsonNotEquals5() throws IOException {
        var jsonComparisonBuilder =
                JsonAssertions.JsonComparisonBuilder.newBuilder()
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("boolean")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("int")
                                                    .addPathForWhichAssumeDefaultValueWhenComparing("double");

        JsonNode json1 = loadJsonFromFile("testAssertJsonNotEquals5-First.json");
        JsonNode json2 = loadJsonFromFile("testAssertJsonNotEquals5-Second.json");

        assertException(
            () -> JsonAssertions.assertJsonEquals(json1, json2, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("fieldWithWrongValue=hello: Expected hello but got world",
                                                  "int=0: Attribute in expected but not in actual",
                                                  "double=0.0: Attribute in expected but not in actual",
                                                  "string=: Attribute in expected but not in actual")));

        printSeparator();
        assertException(
            () -> JsonAssertions.assertJsonEquals(json2, json1, jsonComparisonBuilder),
            JsonComparisonAssertionError.class,
            jcae -> assertThat(jcae.getErrors(),
                               containsInAnyOrder("fieldWithWrongValue=world: Expected world but got hello",
                                                  "int=0: Attribute in actual but not in expected",
                                                  "double=0.0: Attribute in actual but not in expected",
                                                  "string=: Attribute in actual but not in expected")));
    }

    private static JsonNode loadJsonFromFile(String path) throws IOException {
        return new ObjectMapper().readTree(new File("src/test/java/org/sn/myutils/testutils/json/jackson/" + path));
    }

    private static void printSeparator() {
        System.err.println("\n-----------------------------------------------------\n");
    }
}
