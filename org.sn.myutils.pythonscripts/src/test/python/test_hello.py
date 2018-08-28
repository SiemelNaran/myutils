import os
import subprocess
import unittest


class HelloTestCase(unittest.TestCase):
    def test(self):
        process = subprocess.Popen(["python3", "../../main/python/hello.py"],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)
        stdout, stderr = process.communicate()
        self.assertEqual("", stderr)
        self.assertEqual("hello\n", stdout)


if __name__ == '__main__':
    unittest.main()
