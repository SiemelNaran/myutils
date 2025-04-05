package org.sn.myutils.pubsub;

public class Checkstyle {
    record Teacher(String speciality) { }
    record Student(String grade) { }

    public static String action(Object obj) {
        return switch (obj) {
            case Teacher(String s) -> s;
            case Student(String g) -> g;
            default -> "unknown";
        };
    }
}
