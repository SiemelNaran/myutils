module org.sn.myutils.pubsub {
    requires org.sn.myutils.core;    
    exports org.sn.myutils.pubsub;
    
    // for unit tests
    // TODO: need to find a better way and get rid off org.sn.myutils.testutil.Unused.java
//    opens org.sn.myutils.testutils;
//    opens org.sn.myutils.util;
//    opens org.sn.myutils.util.concurrent;
//    opens org.sn.myutils.parsetree;
//    opens org.sn.myutils.pubsub;
}