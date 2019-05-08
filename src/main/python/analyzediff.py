#!/usr/bin/python3

class Diff:
   def __init__(self, before, old, after, new):
       self.before = before
       self.old = old
       self.after = after
       self.new = new

   def __eq__(self, other):
       return self.old == other.old and self.new == other.new

   def __hash__(self):
       return hash(self.old, self.new)


class CalcDiff:
   def __init__(self, first, second):
       self.diffs = CalcDiff.compare(first, second)

   def compare(first, second):
       return [Diff("", "Integer")]
