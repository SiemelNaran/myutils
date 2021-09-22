package org.sn.myutils.jsontestutils.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import org.sn.myutils.jsontestutils.JsonComparisonAssertionError;


/**
 * Class that compares two json objects recursively, pointing out the changes, such as
 * - attributes with differing values
 * - attributes and elements present in one json object, but not in the other.
 *
 * <p>Some of the capabilities include:
 * - ability to treat JSON array as map so that we can compare objects with the same id
 * - ability to not compare certain attributes (such as timeUpdated field) as it will change from run to run
 * - ability to assume missing attribute is set to its default value (so that an attribute missing is same as attribute=default)
 */
class JsonComparator {

    private static final ObjectWriter PRINTER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    /**
     * Class representing path to a JSON attribute.
     */
    private static class JsonPath {
        private static final JsonPath EMPTY = new JsonPath(null, null, null);

        private final String name;
        private final String value;
        private final JsonPath parent;

        private JsonPath(String name, String value, JsonPath parent) {
            this.name = name;
            this.value = value;
            this.parent = parent;
        }

        /**
         * Return the JSON path, which is a string like "[].child.someArray[].booleanFlag" or "[].lastName".
         * These are the same strings passed in to JsonComparisonParametersBuilder.
         */
        String getType() {
            StringBuilder result = new StringBuilder();
            if (parent != null) {
                result.append(parent.getType());
                if (result.length() > 0 && !name.equals("[]")) {
                    result.append('.');
                }
                result.append(name);
            }
            return result.toString();
        }

        /**
         * Return a string like "[recordId=r1].lastName=MyLastName".
         * These are used in the error message.
         */
        String getPathAsString() {
            StringBuilder result = new StringBuilder();
            if (parent != null) {
                result.append(parent.getPathAsString());
                if (result.length() > 0 && !name.equals("[]")) {
                    result.append('.');
                }
                if ("[]".equals(name)) {
                    assert value != null;
                    if (!value.isEmpty()) {
                        result.append(value);
                    }
                } else {
                    if (value == null) {
                        result.append(name);
                    } else {
                        result.append(name).append('=').append(value);
                    }
                }
            }
            return result.toString();
        }

        JsonPath newPath(String name, /*@Nullable*/ String value) {
            return new JsonPath(name, value, this);
        }
    }

    private final boolean printToStdErr;
    private final Map<String /*type*/, String /*idField*/> pathToIdFieldMap;
    private final Collection<String> pathForWhichDontCompareValueList;
    private final Collection<String> pathForWhichAssumeDefaultValueWhenComparingList;

    /**
     * Tool to compare two JSON objects.
     *
     * @param pathToIdFieldMap map of type path to attribute name of unique key, used to order elements of an array node
     * @param pathForWhichDontCompareValueList attributes with these names will not be compared, but we will compare the existence of the field value and field type
     * @param pathForWhichAssumeDefaultValueWhenComparingList attributes with these names will be compared, but if missing the attribute will assume the default value
     */
    JsonComparator(boolean printToStdErr,
                   Map<String, String> pathToIdFieldMap,
                   Collection<String> pathForWhichDontCompareValueList,
                   Collection<String> pathForWhichAssumeDefaultValueWhenComparingList) {
        this.printToStdErr = printToStdErr;
        this.pathToIdFieldMap = pathToIdFieldMap;
        this.pathForWhichDontCompareValueList = pathForWhichDontCompareValueList;
        this.pathForWhichAssumeDefaultValueWhenComparingList = pathForWhichAssumeDefaultValueWhenComparingList;
    }

    void compareAndThrow(JsonNode expected, JsonNode actual) {
        Comparer comparer = new Comparer();
        comparer.compare(JsonPath.EMPTY, expected, actual);
        comparer.throwIfAssertionErrors(actual);
    }

    private class Comparer {
        private final List<String> errors = new ArrayList<>();

        private void compare(JsonPath path, JsonNode expected, JsonNode actual) {
            if (!equivalent(expected.getClass(), actual.getClass())) {
                addError(path,
                         "Expected " + expected.getClass().getSimpleName() + "(" + expected.asText()
                                 + ") but got " + actual.getClass().getSimpleName() + "(" + actual.asText() + ")");
            } else if (expected instanceof ValueNode && actual instanceof ValueNode) {
                if (!Objects.equals(expected.asText(), actual.asText())) {
                    addError(path, "Expected " + expected.asText() + " but got " + actual.asText());
                }
            } else if (actual instanceof ObjectNode) {
                assert expected instanceof ObjectNode;
                compareObjects(path, (ObjectNode) expected, (ObjectNode) actual);
            } else if (actual instanceof ArrayNode) {
                assert expected instanceof ArrayNode;
                compareArrays(path, (ArrayNode) expected, (ArrayNode) actual);
            } else {
                if (!expected.equals(actual)) {
                    addError(path, "Expected " + expected + " but got " + actual);
                }
            }
        }

        private void compareArrays(JsonPath basePath, ArrayNode expectedArray, ArrayNode actualArray) {
            if (expectedArray.size() != actualArray.size()) {
                addError(basePath, "Expected JSON with " + expectedArray.size() + " elements, but got JSON with " + actualArray.size() + " elements");
            }

            String idField = pathToIdFieldMap.get(basePath.getType());

            if (idField != null) {
                compareArraysById(basePath, expectedArray, actualArray, idField);
            } else {
                compareArraysByOrder(basePath, expectedArray, actualArray);
            }
        }

        private void compareArraysById(JsonPath basePath, ArrayNode expectedArray, ArrayNode actualArray, String idField) {
            for (JsonNode eNode : expectedArray) {
                ObjectNode expected = (ObjectNode) eNode;

                String idValue = getFieldAsText(expected, idField);
                JsonPath path = basePath.newPath("[]", "[" + idField + "=" + idValue + "]");
                List<ObjectNode> actualList = new ArrayList<>();
                for (JsonNode aNode : actualArray) {
                    ObjectNode a = (ObjectNode) aNode;
                    if (idValue.equals(getFieldAsText(a, idField))) {
                        actualList.add(a);
                    }
                }
                if (actualList.isEmpty()) {
                    addError(path, "Record not in actual");
                    continue;
                } else if (actualList.size() > 1) {
                    addError(path, "Too many records in actual");
                }

                for (ObjectNode actual: actualList) {
                    compareObjects(path, expected, actual);
                }
            }
        }

        private void compareArraysByOrder(JsonPath basePath, ArrayNode expectedArray, ArrayNode actualArray) {
            int minSize = Math.min(expectedArray.size(), actualArray.size());
            for (int i = 0; i < minSize; i++) {
                JsonNode eNode = expectedArray.get(i);
                JsonNode aNode = actualArray.get(i);
                JsonPath path = basePath.newPath("[]", "[" + i + "]");
                compare(path, eNode, aNode);
            }
        }

        private void compareObjects(JsonPath basePath, ObjectNode expected, ObjectNode actual) {
            Set<String> done = new HashSet<>();
            for (Iterator<Map.Entry<String, JsonNode>> iter = expected.fields(); iter.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = iter.next();
                JsonNode e = entry.getValue();
                String attributeName = entry.getKey();
                JsonNode a = actual.get(attributeName);
                done.add(attributeName);
                JsonPath path = basePath.newPath(attributeName, toSimpleText(e));
                if (a == null) {
                    if (!pathForWhichAssumeDefaultValueWhenComparingList.contains(path.getType()) || isNotDefaultValue(e)) {
                        addError(path, "Attribute in expected but not in actual");
                    }
                } else {
                    if (pathForWhichDontCompareValueList.contains(path.getType())) {
                        compareTypes(path, e, a);
                    } else {
                        compare(path, e, a);
                    }
                }
            }
            for (Iterator<Map.Entry<String, JsonNode>> iter = actual.fields(); iter.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = iter.next();
                String attributeName = entry.getKey();
                if (done.contains(attributeName)) {
                    continue;
                }
                JsonNode a = entry.getValue();
                JsonPath path = basePath.newPath(attributeName, toSimpleText(a));
                if (!pathForWhichAssumeDefaultValueWhenComparingList.contains(path.getType()) || isNotDefaultValue(a)) {
                    addError(path, "Attribute in actual but not in expected");
                }
            }
        }

        private void compareTypes(JsonPath path, JsonNode expected, JsonNode actual) {
            if (!equivalent(expected.getClass(), actual.getClass())) {
                addError(path,
                         "Expected type " + expected.getClass().getSimpleName()
                                 + " but got type " + actual.getClass().getSimpleName());
            }
        }

        private void addError(JsonPath path, String message) {
            errors.add(path.getPathAsString() + ": " + message);
        }

        void throwIfAssertionErrors(JsonNode actual) {
            if (errors.size() > 0) {
                try {
                    if (printToStdErr) {
                        errors.forEach(System.err::println);
                        System.err.println("\nACTUAL:");
                        String actualAsString = PRINTER.writeValueAsString(actual);
                        System.err.println(actualAsString);
                    }
                    throw new JsonComparisonAssertionError(errors);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static String toSimpleText(JsonNode node) {
        if (node instanceof ValueNode) {
            return node.asText();
        } else {
            return null;
        }
    }

    private static /*@Nonnull*/ String getFieldAsText(ObjectNode object, String keyField) {
        JsonNode node = object;
        for (StringTokenizer tokenizer = new StringTokenizer(keyField, "."); tokenizer.hasMoreTokens(); ) {
            String field = tokenizer.nextToken();
            node = node.get(field);
            if (node == null) {
                return "";
            }
        }
        return node.asText();
    }

    /**
     * Return true is the node is not equal to its default value.
     * For BooleanNode return true if the node has value true (as the default value for a boolean field is false).
     * For all other cases return true.
     */
    private static boolean isNotDefaultValue(JsonNode node) {
        if (node instanceof BooleanNode booleanNode) {
            return booleanNode.asBoolean();
        }
        return true;
    }

    /**
     * Return true if the node types are the same, for example if both are TextNode.
     * Also return true if one is an IntNode and the other a LongNode.
     */
    private static boolean equivalent(Class<? extends JsonNode> first, Class<? extends JsonNode> second) {
        if (first.equals(second)) {
            return true;
        }
        if ((first.equals(IntNode.class) && second.equals(LongNode.class))
                || (first.equals(LongNode.class) && second.equals(IntNode.class))) {
            return true;
        }
        if ((first.equals(FloatNode.class) && second.equals(DoubleNode.class))
                || (first.equals(DoubleNode.class) && second.equals(FloatNode.class))) {
            return true;
        }
        return false;
    }
}
