import sys
import traceback

from itertools import takewhile

sys.path.append("../../main/python")
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
    asserter = AssertionFailures()

    try:
        # empty string
        replacements = Replacements("", "")
        asserter.assert_equals("[]", str(replacements))

        # strings identical
        replacements = Replacements("abc", "abc")
        asserter.assert_equals("[]", str(replacements))

        # add entire string
        replacements = Replacements("", "abc")
        asserter.assert_equals("[range(0, 0) -> abc]", str(replacements))

        # delete entire string
        replacements = Replacements("abc", "")
        asserter.assert_equals("[range(0, 3) -> ]", str(replacements))

        # replace entire string
        replacements = Replacements("ab", "cdef")
        asserter.assert_equals("[range(0, 2) -> cdef]", str(replacements))

        # add text in middle (one addition)
        replacements = Replacements("xgetone", "xgetnowtrone")
        asserter.assert_equals("[range(4, 4) -> nowtr]", str(replacements))

        # replace text in middle (one replacement)
        replacements = Replacements("xgetABone", "xgetnowtrone")
        asserter.assert_equals("[range(4, 6) -> nowtr]", str(replacements))

        # delete text in middle (one deletion)
        replacements = Replacements("xgetnowtrone", "xgetone")
        asserter.assert_equals("[range(4, 9) -> ]", str(replacements))

        # add text at end (one addition)
        replacements = Replacements("ab", "abcdef")
        asserter.assert_equals("[range(2, 2) -> cdef]", str(replacements))

        # replace text at end (one replacement)
        replacements = Replacements("xgetoneAB", "xgetonenowtr")
        asserter.assert_equals("[range(7, 9) -> nowtr]", str(replacements))

        # delete text at end (one deletion)
        replacements = Replacements("xgetonenowtr", "xgetone")
        asserter.assert_equals("[range(7, 12) -> ]", str(replacements))

        # add text at start (one addition)
        replacements = Replacements("ab", "cdefab")
        asserter.assert_equals("[range(0, 0) -> cdef]", str(replacements))

        # replace text at start (one replacement)
        replacements = Replacements("ABxgetone", "nowtrxgetone")
        asserter.assert_equals("[range(0, 2) -> nowtr]", str(replacements))

        # delete text at start (one deletion)
        replacements = Replacements("nowtrxgetone", "xgetone")
        asserter.assert_equals("[range(0, 5) -> ]", str(replacements))

        # longest common substring
        replacements = Replacements("abcde", "-ab+bcd*")
        # longest common substring is "bcd" so insert "b+" after 1st char, and replace last char "e" with "*"
        # in other words: asserter.assert_equals("[range(0, 0) -> -, range(1, 1) -> b+, range(4, 5) -> *]", str(replacements))
        # but our algorithm does not find the longest common substring 
        # instead the only strings found are "ab" and "cd"
        asserter.assert_equals("[range(0, 0) -> -, range(2, 2) -> +b, range(4, 5) -> *]", str(replacements))

    finally:
        asserter.throw_if_failures()


def test_equivalent():
    asserter = AssertionFailures()

    try:
        replacements1 = Replacements("int val = getValue();", "int val = getIntegerValue();")
        replacements2 = Replacements("System.out.println(getValue());", "System.out.println(getIntegerValue());")
        asserter.assert_equals(1, len(replacements1.replacements))
        asserter.assert_equals(1, len(replacements2.replacements))
        asserter.assert_true(replacements1.replacements[0].equivalent(replacements2.replacements[0]))

    finally:
        asserter.throw_if_failures()


def main():
    test_replacements()
    test_equivalent()

if __name__ == "__main__":
  main()
