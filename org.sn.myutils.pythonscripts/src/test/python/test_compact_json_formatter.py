import os
import subprocess
import unittest


def create_process(threshold, stdin_pipe):
    args = ["python3", "../../main/python/compact_json_formatter.py"]
    if threshold:
        args.append("--threshold")
        args.append(str(threshold))
    if not stdin_pipe:
        args.append("--nostdin")
        args.append("test_compact_json_formatter-input01.json")
    return subprocess.Popen(args,
                            stdin=stdin_pipe,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True)


class CompactJsonFormatterTestCase(unittest.TestCase):
    def test_01_80_file(self):
        process = create_process(threshold=None, stdin_pipe=None)
        stdout, stderr = process.communicate()
        self.assertEqual("", stderr)
        with open("test_compact_json_formatter-output01-80.json", "r") as expected_file:
            expected = expected_file.read()
            self.assertEqual(expected, stdout)

    def test_01_44_file(self):
        process = create_process(threshold=44, stdin_pipe=None)
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
            process = create_process(threshold=None, stdin_pipe=stdin_pipe)
            stdout, stderr = process.communicate()
            self.assertEqual("", stderr)
            with open("test_compact_json_formatter-output01-80.json", "r") as expected_file:
                expected = expected_file.read()
                self.assertEqual(expected, stdout)


if __name__ == '__main__':
    unittest.main()