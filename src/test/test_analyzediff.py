import sys

sys.path.append("../../src/main/sh")
from analyzediff import CalcDiff

def main():
    CalcDiff("    int a = thing.getValue() + 1;",
             "        int a = thing.getIntegerValue() + 1;")
    print("Hello")


if __name__ == "__main__":
  main()
