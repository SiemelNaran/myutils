import os
import subprocess
import unittest

from src.main.python.compact_json_formatter import Line


def create_process(threshold, indent, stdin_pipe):
    args = ["python3", "../../main/python/compact_json_formatter.py"]
    if threshold:
        args.append("--threshold")
        args.append(str(threshold))
    if indent:
        args.append("--indent")
        args.append(str(indent))
    if not stdin_pipe:
        args.append("--nostdin")
        args.append("test_compact_json_formatter-input01.json")
    return subprocess.Popen(args,
                            stdin=stdin_pipe,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True)


class CompactJsonFormatterTestCase(unittest.TestCase):
    def test_empty(self):
        line = Line(0, "{", Line.LINE_FLAG_OPENING)
        nextline = Line(0, "}", Line.LINE_FLAG_CLOSING)
        self.assertEqual(True, line.try_merge_same_level(nextline, 80))
        self.assertEqual(0, line.indent)
        self.assertEqual("{ }", line.content)
        self.assertEqual(0, line.flags)

    def test_three(self):
        line = Line(0, "{", Line.LINE_FLAG_OPENING)
        nextline = Line(1, "\"hello\":\"world\"", 0)
        nextline2 = Line(0, "}", Line.LINE_FLAG_CLOSING)

        # prove that merging line with nextline does not work
        self.assertEqual(False, line.try_merge_same_level(nextline, 80))
        self.assertEqual(0, line.indent)
        self.assertEqual("{", line.content)
        self.assertEqual(line.LINE_FLAG_OPENING, line.flags)

        # prove that merging line with next two line works
        self.assertEqual(True, line.try_merge_nestedlevel(nextline, nextline2, 80))
        self.assertEqual(0, line.indent)
        self.assertEqual("{ \"hello\":\"world\" }", line.content)
        self.assertEqual(0, line.flags)

    def test_01_80_file(self):
        process = create_process(threshold=None, indent=None, stdin_pipe=None)
        stdout, stderr = process.communicate()
        self.assertEqual("", stderr)
        with open("test_compact_json_formatter-output01-80.json", "r") as expected_file:
            expected = expected_file.read()
            self.assertEqual(expected, stdout)

    def test_01_44_file(self):
        process = create_process(threshold=44, indent=None, stdin_pipe=None)
        stdout, stderr = process.communicate()
        self.assertEqual("", stderr)
        with open("test_compact_json_formatter-output01-44.json", "r") as expected_file:
            expected = expected_file.read()
            self.assertEqual(expected, stdout)

    def test_01_80_stdin(self):
        with open("test_compact_json_formatter-input01.json", "r") as expected_file:
            input_string = expected_file.read()
            stdin_pipe, stdin_write_pipe = os.pipe()
            os.write(stdin_write_pipe, input_string.encode())
            os.close(stdin_write_pipe)
            process = create_process(threshold=None, indent=None, stdin_pipe=stdin_pipe)
            stdout, stderr = process.communicate()
            self.assertEqual("", stderr)
            with open("test_compact_json_formatter-output01-80.json", "r") as expected_file:
                expected = expected_file.read()
                self.assertEqual(expected, stdout)

    def test_01_80_file_indent(self):
        process = create_process(threshold=None, indent=2, stdin_pipe=None)
        stdout, stderr = process.communicate()
        self.assertEqual("", stderr)
        with open("test_compact_json_formatter-output01-80-indent.json", "r") as expected_file:
            expected = expected_file.read()
            self.assertEqual(expected, stdout)


if __name__ == '__main__':
    unittest.main()