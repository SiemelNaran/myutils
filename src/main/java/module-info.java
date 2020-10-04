module org.sn.myutils.core {
    requires jsr305; // results in warning: Required filename-based automodules detected. Please don't publish this project to a public artifact repository!
    requires transitive java.logging;
    
    exports org.sn.myutils.util;
    exports org.sn.myutils.util.concurrent;
    exports org.sn.myutils.parsetree;
    exports org.sn.myutils.pubsub;
    
    // for unit tests
    // TODO: need to find a better way and get rid off org.sn.myutils.testutil.Unused.java
    opens org.sn.myutils.testutils;
    opens org.sn.myutils.util;
    opens org.sn.myutils.util.concurrent;
    opens org.sn.myutils.parsetree;
    opens org.sn.myutils.pubsub;
}
