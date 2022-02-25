from argparse import ArgumentParser
from collections import deque, namedtuple
import json
import numbers
import sys


def main():
    args = parseargs()
    if "filename" in args:
        lines = readlines_from_file(args.filename)
    else:
        lines = readlines_from_stdin()
    compactify(lines, args.threshold)
    for line in lines:
        print(str(line))


def parseargs():
    filename_required = sys.stdin.isatty()
    parser = ArgumentParser(description="Format json in a compact way, "
                                        "putting JSON objects and arrays on the same line if they are short")
    if filename_required:
        parser.add_argument("filename", type=str, help="the JSON file, required unless reading from standard input")
    parser.add_argument("--threshold", type=int, default=80, required=False, help="the threshold, default 80")
    return parser.parse_args()


def readlines_from_file(filename):
    with open(filename) as file:
        return readlines(file)


def readlines_from_stdin():
    return readlines(sys.stdin)


def readlines(fp):
    tree = json.load(fp)
    return dumplines(tree)


class Line:
    LINE_FLAG_OPENING = 1
    LINE_FLAG_CLOSING = 2

    indent: int
    content: str
    flags: int

    def __init__(self, indent, content, flags):
        self.indent = indent
        self.content = content
        self.flags = flags

    def __str__(self):
        return ("    " * self.indent) + self.content

    def is_opening(self):
        return self.flags & Line.LINE_FLAG_OPENING

    def is_closing(self):
        return self.flags & Line.LINE_FLAG_CLOSING

    def try_merge_same_level(self, nextline, threshold):
        """
        Merge this line with the next line if
        - both lines have the same indentation
        - their combined length does not exceed the threshold.
        - you are not merging a value with an object/array

        So change something like
           "hello": 1,
           "world": 2
        into
           "hello": 1, "world": 2
        """
        if self.indent == nextline.indent and \
                not self.is_closing() and not nextline.is_opening() and \
                len(self.content) + 1 + len(nextline.content) <= threshold:
            self.content += " "
            self.content += nextline.content
            return True
        return False

    def try_merge_nestedlevel(self, nextline, nextline2, threshold):
        """
        Merge this line with the next two lines if
        - the first and last line have the same indentation
        - the first line opens an element and the last line closes and element
        - their combined length does not exceed the threshold.

        So change something like
           "random": {
             "hello": 1, "world": 2
           }
        into
           "random": { "hello": 1, "world": 2 }

        and change
            {
            }
        into
            { }
        """
        if self.is_opening() and nextline2.is_closing() and \
                self.indent == nextline2.indent and self.indent + 1 == nextline.indent and \
                len(self.content) + 1 + len(nextline.content) + 1 + len(nextline2.content) <= threshold:
            self.content += " "
            self.content += nextline.content
            self.content += " "
            self.content += nextline2.content
            return True
        return False


ContextualNode = namedtuple("ContextualNode", "node indent comma")
Attribute = namedtuple("Attribute", "key value")
ClosingString = namedtuple("ClosingString", "string")  # represents the closing square bracket or curly brace


def dumplines(root):
    """
    Write root node using queue, in order to avoid stack overflow errors.
    Return a list of lines.
    """

    lines = []
    queue = deque([ContextualNode(root, 0, comma=False)])
    while len(queue):
        contextual_node = queue.popleft()
        line = dumpline(contextual_node, queue)
        lines.append(line)
    return lines


def dumpline(contextual_node, queue):
    """
    Write node
    Return a line.
    Put the children of contextual_node into the queue.
    """

    indent = contextual_node.indent
    nextindent = indent + 1

    node = contextual_node.node
    comma = contextual_node.comma

    content = ""
    flags = 0

    if type(node) is Attribute:
        content += '"'
        content += node.key
        content += '": '
        node = node.value

    if type(node) is list:
        content += "["
        flags = Line.LINE_FLAG_OPENING
        queue.appendleft(ContextualNode(ClosingString("]"), indent, comma=comma))
        for i, elem in enumerate(reversed(node)):
            not_last_element = i > 0
            queue.appendleft(ContextualNode(elem, nextindent, comma=not_last_element))
    elif type(node) is dict:
        content += "{"
        flags = Line.LINE_FLAG_OPENING
        queue.appendleft(ContextualNode(ClosingString("}"), indent, comma=comma))
        for i, entry in enumerate(reversed(node.items())):
            not_last_element = i > 0
            queue.appendleft(ContextualNode(Attribute(entry[0], entry[1]), nextindent, comma=not_last_element))
    else:
        if type(node) is str:
            content += '"'
            content += node
            content += '"'
        elif type(node) is bool:
            content += "true" if node else "false"
        elif isinstance(node, numbers.Number):
            content += str(node)
        elif type(node) is ClosingString:
            content += node.string
            flags = Line.LINE_FLAG_CLOSING

        if comma:
            content += ','

    return Line(indent, content, flags)


def compactify(lines, threshold):
    """
    If subsequent lines in the same indent level together are less than the threshold,
    then combine the lines into one.
    """

    i = 1

    while i < len(lines):
        prevline2 = lines[i - 2] if i >= 2 else None
        prevline = lines[i - 1]
        line = lines[i]
        if prevline.try_merge_same_level(line, threshold):
            lines.pop(i)
            i -= 1
        elif prevline2 and prevline2.try_merge_nestedlevel(prevline, line, threshold):
            lines.pop(i - 1)
            lines.pop(i - 1)
            i -= 2
        i += 1


if __name__ == '__main__':
    main()
