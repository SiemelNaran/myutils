package org.sn.myutils.testutils.json.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class JsonAssertions {
    /**
     * Compare two json objects.
     *
     * @throws org.sn.myutils.testutils.json.JsonComparisonAssertionError if objects are not equal
     */
    public static void assertJsonEquals(JsonNode expected, JsonNode actual, JsonComparisonParametersBuilder builder) {
        var jsonComparator = builder.build();
        jsonComparator.compareAndThrow(expected, actual);
    }

    public static class JsonComparisonParametersBuilder {
        private boolean printToStdErr;
        private final Map<String /*type*/, String /*sortField*/> pathToIdFieldMap = new HashMap<>();
        private final Collection<String> pathForWhichDontCompareValueList = new ArrayList<>();
        private final Collection<String> pathForWhichAssumeDefaultValueWhenComparingList = new ArrayList<>();

        public static JsonComparisonParametersBuilder newBuilder() {
            return new JsonComparisonParametersBuilder();
        }

        /**
         * Tell whether to print differences to stderr as well as in the AssertionError.
         * This is useful because exception message may be hard to read.
         */
        public JsonComparisonParametersBuilder setPrintToStdErr(boolean value) {
            this.printToStdErr = value;
            return this;
        }

        /**
         * When comparing two JSON arrays of objects where one attribute represents the id of the object,
         * the order of records may be different.
         * Use the id field to match which JSON objects to compare.
         *
         * <p>Examples<ul>
         *     <li>
         *         <code>addPathToIdField("", "record_id")</code>
         *         if top level is a JSON array of objects, each having an attribute record_id
         *     </li>
         *     <li>
         *         <code>addPathToIdField("[].child.someArray", "id")</code>
         *         if top level is a JSON array and each object has a child object "child", which has an attribute "someArray" which is a JSON array
         *     </li>
         * </ul>
         */
        public JsonComparisonParametersBuilder addPathToIdField(String jsonPath, String idField) {
            pathToIdFieldMap.put(jsonPath, idField);
            return this;
        }

        /**
         * Add the path we should not compare.
         * For example, there may be an attribute like timeUpdated that will naturally differ between expected and actual output.
         * However, the code will test for the existence of the attribute, and that the types are equivalent.
         *
         * <p>Examples<ul>
         *     <li>
         *         <code>addPathForWhichDontCompareValue("[].timeUpdated")</code>
         *         if top level is a JSON array of objects,
         *         each having an attribute "timeUpdated",
         *     </li>
         * </ul>
         */
        public JsonComparisonParametersBuilder addPathForWhichDontCompareValue(String jsonPath) {
            pathForWhichDontCompareValueList.add(jsonPath);
            return this;
        }

        /**
         * Add path we should compare, where is one object does not exist we use the default value.
         * For example, one JSON may have a boolean attribute set to false, and the other has the boolean attribute missing,
         * yet they should be considered equal.
         *
         * <p>The only supported types are
         * <ul>
         *     <li>boolean, where missing means false</li>
         * </ul>
         *
         * <p>Examples<ul>
         *     <li>
         *         <code>addPathForWhichAssumeDefaultValueWhenComparing("[].child.someArray[].booleanFlag")</code>
         *         if top level is a JSON array of objects,
         *         each having an attribute "child" which is a JSON object,
         *         which has an attribute "someArray" which is a JON array,
         *         which has an attrubute "booleanFlag".
         *     </li>
         * </ul>
         */
        public JsonComparisonParametersBuilder addPathForWhichAssumeDefaultValueWhenComparing(String type) {
            pathForWhichAssumeDefaultValueWhenComparingList.add(type);
            return this;
        }

        private JsonComparator build() {
            return new JsonComparator(printToStdErr,
                                      pathToIdFieldMap,
                                      pathForWhichDontCompareValueList,
                                      pathForWhichAssumeDefaultValueWhenComparingList);
        }
    }
}
