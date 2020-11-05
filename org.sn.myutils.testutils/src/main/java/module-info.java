@SuppressWarnings({ "requires-automatic", "requires-transitive-automatic" })
module org.sn.myutils.testutils {
    requires transitive org.junit.jupiter.api;
    requires org.junit.jupiter.params;
    requires transitive org.hamcrest;
    exports org.sn.myutils.testutils;
}
