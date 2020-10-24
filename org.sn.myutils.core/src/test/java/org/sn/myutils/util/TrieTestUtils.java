package org.sn.myutils.util;

import java.util.ArrayList;
import java.util.Collection;

class TrieTestUtils {
    static String[] WORDS = {
            "bottle",
            "bottom",
            "boat",
            "apple",
            "bat",
            "bottomless"
    };

    static String[] NON_WORDS = {
            "box",
            "orange"
    };

    static Collection<Character> toCollectionChar(String str) {
        Collection<Character> result = new ArrayList<>(str.length());
        str.chars().forEach(ch -> result.add((char) ch));
        return result;
    }

}
