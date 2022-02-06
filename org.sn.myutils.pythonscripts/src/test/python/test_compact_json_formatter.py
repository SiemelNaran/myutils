import os
import subprocess
import unittest


class CompactJsonFormatterTestCase(unittest.TestCase):
    def setUp(self):
        self.process = subprocess.Popen(["python3",
                                         "../../main/python/compact_json_formatter.py",
                                         "test_compact_json_formatter-input01.json"],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        universal_newlines=True)

    def test_01_80(self):
        stdout, stderr = self.process.communicate()
        self.assertEqual("", stderr)
        with open("test_compact_json_formatter-output01-80.json", "r") as expected_file:
            expected = expected_file.read()
            self.assertEqual(expected, stdout)

    def test_01_44(self):
        stdout, stderr = self.process.communicate()
        self.assertEqual("", stderr)
        with open("test_compact_json_formatter-output01-80.json", "r") as expected_file:
            expected = expected_file.read()
            self.assertEqual(expected, stdout)


if __name__ == '__main__':
    unittest.main()
