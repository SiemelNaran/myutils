#!/usr/bin/python3

import itertools

from operator import attrgetter


class Segment:
    """
    Class representing a substring in two strings.

    Members:
    - length (int): The length of the substring, always 1 or more
    - fIndex (int): The index of the last char in the first string
    - sIndex (int): The index of the last char in the second string
    """

    @staticmethod
    def empty_segment_start_of_string():
        segment = Segment(0, -1, -1)
        return segment

    @staticmethod
    def empty_segment_end_of_string(first, second):
        segment = Segment(0, len(first) - 1, len(second) - 1)
        return segment

    def __init__(self, length, fIndex, sIndex):
        self.length = length
        self.fIndex = fIndex
        self.sIndex = sIndex

    def __repr__(self):
        return "(len=%d, %d, %d)" % (self.length, self.fIndex, self.sIndex)


class Group:
    """
    Class representing a sequence of non-contiguous segments.
    Rather than storing a list of segments, we store a pointer to the parent group (if any) and the last segment.

    Members:
    - parent (Group): The parent group, or prior segments. May be None.
    - segment (Segment): The last segment
    """

    def __init__(self, parent, segment):
        self.parent = parent
        self.segment = segment

    def top(self):
        return self.segment

    def get_segments(self):
        if self.parent is None:
            segments = []
        else:
            segments = self.parent.get_segments()
        segments.append(self.segment)
        return segments

    def try_extend(self, fIndex, sIndex):
        top = self.top()
        if top.fIndex + 1 == fIndex and top.sIndex + 1 == sIndex:
            segment = Segment(top.length + 1, top.fIndex + 1, top.sIndex + 1)
            return Group(self.parent, segment)
        else:
            return None

    def __lt__(self, other):
        self_lengths = [segment.length for segment in self.get_segments()]
        other_lengths = [segment.length for segment in other.get_segments()]
        self_lengths.sort(reverse=True)
        other_lengths.sort(reverse=True)
        return self_lengths < other_lengths

    def __repr__(self):
        return str(self.get_segments())


def linediff(first, second):
    """
    Compare two strings and return the similarities as a list of segments.
    A similarity means two substrings of the same characters.

    Running time is O(N^2) where N is the length of each string
    - There is an outer loop for over the characters in the string, so N iterations
    - In the inner loop we call find on the second string, which is O(N), hence O(N^2)
    - In the inner loop we add up to O(N) groups to an array, running time is now like 2*O(N^2) which is still O(N^2)
    - We finally sort the groups using quicksort, so running time like 2*O(N^2) +N*lg(N)

    Return: The best list of segments describing similarities, or the empty list if there are no similarities
    """

    groups = []
    for fIndex, fChar in enumerate(first):
        new_groups = []

        for group in itertools.chain(groups, [None]):
            sIndex = (second.find(fChar, group.top().sIndex + 1) if group is not None
                                                                 else second.find(fChar))
            if sIndex >= 0:
                new_group = group.try_extend(fIndex, sIndex) if group is not None else None
                if new_group is None:
                    new_group = Group(group, Segment(1, fIndex, sIndex))
                new_groups.append(new_group)
                while True:
                    sIndex = second.find(fChar, sIndex + 1)
                    if sIndex == -1:
                        break
                    new_group = Group(group, Segment(1, fIndex, sIndex))
                    new_groups.append(new_group)

        if any(new_groups):
            # of the new segments added, only keep those with the longest length (i.e. only keep segments that have been extended)
            # this optimization saves time and memory, but will not find the longest common substring
            new_segment_max_length = max(map(lambda group: group.top().length, new_groups))
            removeif_inplace(lambda group: group.top().length < new_segment_max_length, new_groups)

            # add the new groups
            groups.extend(new_groups)

            # sort by groups with longest segment first
            groups.sort(reverse=True)

    if not any(groups):
        return []
    else:
        return groups[0].get_segments()


class Replacement:
    """
    Class representing a replacement, addition, or removal of text upon an original string.

    Members:
    - range (range): A range describing the range of text to replace. A length of zero means we are adding text to the original.
    - text (str): The new text. The empty string means we are removing chars from the original.
    """

    def __init__(self, range, text):
        self.range = range
        self.text = text

    def equivalent(self, other):
        return len(self.range) == len(other.range) and self.text == other.text

    def __repr__(self):
        return str(self.range) + " -> " + self.text


class Replacements:
    def __init__(self, first, second):
        self.replacements = []
        if len(first) > 0 or len(second) > 0:
            segments = itertools.chain([Segment.empty_segment_start_of_string()],
                                       linediff(first, second),
                                       [Segment.empty_segment_end_of_string(first, second)])
            prev_segment = next(segments)
            for segment in segments:
                old_range = range(prev_segment.fIndex + 1, segment.fIndex - segment.length + 1)
                new_text = second[prev_segment.sIndex + 1 : segment.sIndex - segment.length + 1]
                if len(old_range) > 0 or len(new_text) > 0:
                    self.replacements.append(Replacement(old_range, new_text))
                prev_segment = segment

    def __repr__(self):
        return str(self.replacements)


def removeif_inplace(predicate, array):
    """
    removes elements from array in place that match the given predicate
    running time O(N)
    """
    i = 0
    for i, elem in enumerate(array):
        if predicate(elem):
            __removeif_inplace_internal(predicate, array, i + 1)
    return

def __removeif_inplace_internal(predicate, array, i):
    offset = 1
    length = len(array)
    while i < length:
        if predicate(array[i]):
            offset += 1
        else:
            array[i - offset] = array[i]
        i += 1
    del array[length - offset : length]

