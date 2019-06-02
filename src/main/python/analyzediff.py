#!/usr/bin/python3

import itertools

from collections import defaultdict
from operator import attrgetter


class Segment:
    """
    Class representing a substring in two strings.

    Members:
    - length (int): The length of the substring
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

    def contains_sIndex(self, sIndex):
        if self.sIndex - self.length < sIndex and sIndex <= self.sIndex:
            return True
        else:
            return False

    def __repr__(self):
        return "(len=%d, %d, %d)" % (self.length, self.fIndex, self.sIndex)


class Group:
    """
    Class representing a sequence of non-contiguous segments.
    Rather than storing a list of segments, we store a pointer to the parent group (if any) and the last segment.

    Members:
    - parent (Group): The parent group, or prior segments. May be None.
    - negative_num_segments (int): The number of segments as a negative number (negative for sorting)
    - segment (Segment): The last segment
    - total_length (int): The lengths of all segments summed together
    - max_segment (int): The maximum length of all segments
    """

    def __init__(self, parent, segment):
        self.parent = parent
        self.negative_num_segments = (parent.negative_num_segments if parent else 0) - 1
        self.segment = segment
        self.total_length = (parent.total_length if parent else 0) + segment.length
        self.max_segment = max(parent.max_segment, segment.length) if parent else segment.length

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
            top.length += 1
            top.fIndex += 1
            top.sIndex += 1
            self.total_length += 1
            self.max_segment = max(self.max_segment, top.length)
            return True
        else:
            return False

    def __repr__(self):
        return str(self.get_segments())


def linediff(first, second):
    """
    Compare two strings and return the similarities as a list of segments.
    A similarity means two substrings of the same characters.

    Running time is O(N^2) where N is the length of each string
    - There is an outer loop for over the characters in the string, so N iterations
    - In the inner loop we iterate over all groups, and in theory there could be O(N) groups as in string1="a" string2="a1a2a3a4a5..."
    - Within this loop call find to find the next occurrence of the letter, hence O(N)
    - So worst case is O(N^3)
    - Within the outer loop we also find the group with smallest length, and delete groups that are too small, so O(N*(2N + N^2)) 
    - Within the outer loop we may also find the next char that is non-contiguous, so O(N*(3N + N^2))
    - Within the outer loop we also sort the groups, so O(N*(2N + N*lg(N) + N^2))

    Return: The best list of segments describing similarities, or the empty list if there are no similarities
    """

    groups = []

    for fIndex, fChar in enumerate(first):
        groups_extended = []
        groups_not_extended = []
        new_groups = []

        # try to extend existing groups with the new char
        # for example if strings are 
        #     xget
        #     -xg-xge
        # and we have matched the x in the first string to both x's in the second string (two groups)
        # then upon reading the g, extend both groups by one char
        for group in groups:
            sIndex = second.find(fChar, group.top().sIndex + 1)
            if sIndex >= 0:
                if group.try_extend(fIndex, sIndex):
                    groups_extended.append(group)
                else:
                    groups_not_extended.append(group)

        # if there are two or more groups and some were not extended, drop the ones that was not extended
        # if its total length is less than the smallest of those groups that were extended
        # for example if strings are 
        #     xget
        #     -xg-xge
        # and we have matched the xg to both xg's in the second string (two groups)
        # then upon reading the e, only the second group was extend, so the first group will be deleted
        # if strings are 
        #     xget5
        #     -get5-xge
        # and we have read the e so there are two groups xge and ge
        # then upon reading the t, there are two groups xge and get, and no groups deleted.
        # upon reading the 5 we have xge and get5, and xge is dropped because its length is less than the other group.
        smallest_total_length = min(map(lambda group: group.total_length, groups_extended)) if any(groups_extended) else 0
        removeif_inplace(lambda group: group.total_length < smallest_total_length, new_groups)

        # for all groups that were not extended, including the empty group representing the start of both strings,
        # create new groups with one more segment
        for group in itertools.chain(groups_not_extended, [None]):
            sIndex = group.top().sIndex if group is not None else -1
            while True:
                sIndex = second.find(fChar, sIndex + 1)
                if sIndex == -1:
                    break
                # if any of the existing groups already contains the char at sIndex then don't create a new group
                # for example in strings
                #     xg1
                #     xg2g
                # Suppose we've read the x.  The first group contains x.
                # When we read g, then we will extend the first group to xg.
                # We then find substrings in the second string starting with g only,
                # but only the second g qualifies. The first g does not qualify as it is already used in xg.
                if any(filter(lambda extended_group: extended_group.top().contains_sIndex(sIndex), groups_extended)):
                    continue
                new_groups.append(Group(group, Segment(1, fIndex, sIndex)))

        # add the new groups
        if any(new_groups):
            groups.extend(new_groups)

        # sort by groups with total length first, then longest segment first, then fewest segments first
        groups.sort(key=attrgetter("total_length", "max_segment", "negative_num_segments"), reverse=True)

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
    """
    Class representing a list of replacements.

    Members:
    - replacements (Replacement[]): list of replacements
    """

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

