import sys

sys.path.append("../../main/python")
from analyzediff import Replacements

def assert_equals(expected, actual):
    if expected != actual:
        print("!!! expected " + str(expected) + " but got " + str(actual))
        print("Exiting")
        sys.exit(1)
        

def main():
    # strings identical
    replacements = Replacements("abc", "abc")
    assert_equals("[]", str(replacements))

    # replace entire string
    replacements = Replacements("ab", "cdef")
    assert_equals("[range(0, 2) -> cdef]", str(replacements))

    # add text in middle (one addition)
    replacements = Replacements("xgetone", "xgetnowtrone")
    assert_equals("[range(4, 4) -> nowtr]", str(replacements))

    # replace text in middle (one replacement)
    replacements = Replacements("xgetABone", "xgetnowtrone")
    assert_equals("[range(4, 6) -> nowtr]", str(replacements))

    # delete text in middle (one deletion)
    replacements = Replacements("xgetnowtrone", "xgetone")
    assert_equals("[range(4, 9) -> ]", str(replacements))

    # add text at end (one addition)
    replacements = Replacements("ab", "abcdef")
    assert_equals("[range(2, 2) -> cdef]", str(replacements))

    # replace text at end (one replacement)
    replacements = Replacements("xgetoneAB", "xgetonenowtr")
    assert_equals("[range(7, 9) -> nowtr]", str(replacements))

    # delete text at end (one deletion)
    replacements = Replacements("xgetonenowtr", "xgetone")
    assert_equals("[range(7, 12) -> ]", str(replacements))

    # add text at start (one addition)
    replacements = Replacements("cdefab", "ab")
    assert_equals("[range(0, 0) -> cdef]", str(replacements))

    # replace text at start (one replacement)
    replacements = Replacements("ABxgetone", "nowtrxgetone")
    assert_equals("[range(0, 2) -> nowtr]", str(replacements))

    # delete text at start (one deletion)
    replacements = Replacements("nowtrxgetone", "xgetone")
    assert_equals("[range(0, 5) -> ]", str(replacements))


if __name__ == "__main__":
  main()
