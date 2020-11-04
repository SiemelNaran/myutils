module org.sn.myutils.core {
    requires transitive jsr305; // results in warning: Required filename-based automodules detected. Please don't publish this project to a public artifact repository!
    exports org.sn.myutils.util;
    exports org.sn.myutils.util.concurrent;
}