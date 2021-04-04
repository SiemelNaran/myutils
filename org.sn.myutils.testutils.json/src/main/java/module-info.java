module org.sn.myutils.core {
    requires transitive com.fasterxml.jackson.databind;
    exports org.sn.myutils.testutils.json;
    exports org.sn.myutils.testutils.json.jackson;
}