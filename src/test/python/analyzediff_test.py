import os
import sys
import traceback

from itertools import takewhile

sys.path.append(os.path.join(sys.path[0], "..", "..", "main", "python"))
from analyzediff import linediff
from analyzediff import Replacements


class AssertionFailures:
    def __init__(self):
        self.num_failures = 0

    def assert_equals(self, expected, actual):
        if expected != actual:
            if self.num_failures == 0:
                print("\n")
            self.num_failures += 1
            print("!!! expected " + str(expected) + " but got " + str(actual))
            stack = takewhile(lambda elem: elem.name != "assert_equals", traceback.extract_stack(limit=2))
            print("".join(traceback.format_list(stack)))

    def assert_true(self, actual):
        if not actual:
            if self.num_failures == 0:
                print("\n")
            self.num_failures += 1
            print("!!! expected True but got False")
            stack = takewhile(lambda elem: elem.name != "assert_true", traceback.extract_stack(limit=2))
            print("".join(traceback.format_list(stack)))

    def throw_if_failures(self):
        if self.num_failures > 0:
            raise AssertionError(str(self.num_failures) + " failures")


        

def test_replacements():
    #assert 1 == 2
    asserter = AssertionFailures()

    try:
        # empty string
        asserter.assert_equals("[]", str(Replacements("", "")))

        # strings identical
        asserter.assert_equals("[]", str(Replacements("abc", "abc")))

        # add entire string
        asserter.assert_equals("[range(0, 0) -> abc]", str(Replacements("", "abc")))

        # delete entire string
        asserter.assert_equals("[range(0, 3) -> ]", str(Replacements("abc", "")))

        # replace entire string
        asserter.assert_equals("[range(0, 2) -> cdef]", str(Replacements("ab", "cdef")))

        # add text in middle (one addition)
        asserter.assert_equals("[range(4, 4) -> nowtr]", str(Replacements("xgetone", "xgetnowtrone")))

        # replace text in middle (one replacement)
        asserter.assert_equals("[range(4, 6) -> nowtr]", str(Replacements("xgetABone", "xgetnowtrone")))

        # delete text in middle (one deletion)
        asserter.assert_equals("[range(4, 9) -> ]", str(Replacements("xgetnowtrone", "xgetone")))

        # add text at end (one addition)
        asserter.assert_equals("[range(2, 2) -> cdef]", str(Replacements("ab", "abcdef")))

        # replace text at end (one replacement)
        asserter.assert_equals("[range(7, 9) -> nowtr]", str(Replacements("xgetoneAB", "xgetonenowtr")))

        # delete text at end (one deletion)
        asserter.assert_equals("[range(7, 12) -> ]", str(Replacements("xgetonenowtr", "xgetone")))

        # add text at start (one addition)
        asserter.assert_equals("[range(0, 0) -> cdef]", str(Replacements("ab", "cdefab")))

        # replace text at start (one replacement)
        asserter.assert_equals("[range(0, 2) -> nowtr]", str(Replacements("ABxgetone", "nowtrxgetone")))

        # delete text at start (one deletion)
        asserter.assert_equals("[range(0, 5) -> ]", str(Replacements("nowtrxgetone", "xgetone")))

        # add text in middle 2 (one addition)
        asserter.assert_equals("[range(0, 1) -> 0, range(4, 4) -> 12ab34]", str(Replacements("abcd", "0bcd12ab34")))

        # longest common substring
        asserter.assert_equals("[range(0, 1) -> -ab+, range(4, 5) -> *]", str(Replacements("abcde", "-ab+bcd*")))
        asserter.assert_equals("[range(0, 1) -> U, range(5, 6) -> UUxgUeUxgeUUtU]", str(Replacements("xget5+", "Uget5UUxgUeUxgeUUtU"))) 

    finally:
        asserter.throw_if_failures()


def test_equivalent():
    #assert 1 == 2
    asserter = AssertionFailures()

    try:
        replacements1 = Replacements("int val = getValue();", "int val = getIntegerValue();")
        replacements2 = Replacements("System.out.println(getValue());", "System.out.println(getIntegerValue());")
        asserter.assert_equals(1, len(replacements1.replacements))
        asserter.assert_equals(1, len(replacements2.replacements))
        asserter.assert_true(replacements1.replacements[0].equivalent(replacements2.replacements[0]))
        #asserter.assert_equals(1, 2)

    finally:
        asserter.throw_if_failures()


if __name__ == "__main__":
    test_replacements()
    test_equivalent()
