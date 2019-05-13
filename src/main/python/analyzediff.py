#!/usr/bin/python3

from itertools import islice


class Segment:
    """
    Class representing a substring in two strings.

    Members:
    - length (int): The length of the substring, may be zero
    - fIndex (int): The index of the last char in the first string
    - sIndex (int): The index of the last char in the second string
    """

    @staticmethod
    def empty_segment():
        segment = Segment(-1, -1)
        segment.length = 0
        return segment

    def __init__(self, fIndex, sIndex):
        self.length = 1
        self.fIndex = fIndex
        self.sIndex = sIndex

    def __repr__(self):
        return "(len=%d, %d, %d)" % (self.length, self.fIndex, self.sIndex)


class Group:
    """
    Class representing a sequence of segments.
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

    def try_extend_segment(self, fIndex, sIndex):
        top = self.top()
        if top.fIndex + 1 == fIndex and top.sIndex + 1 == sIndex:
            top.length += 1
            top.fIndex += 1
            top.sIndex += 1
            return True
        else:
            return False

    def __repr__(self):
        return str(get_segments())

# getone
# getnowone

class LineDiff:
    def __init__(self, first, second):
        self.group = LineDiff.compute_group(first, second)

    @staticmethod
    def compute_group(first, second):
        groups = [Group(None, Segment.empty_segment())]
        for fIndex, fChar in enumerate(first):
            new_groups = []
            for group in groups:
                segment = group.top()
                sIndex = second.find(fChar, segment.sIndex + 1)
                if sIndex >= 0:
                    add_additional_groups = group.length = 0
                    if group.try_extend_segment(fIndex, sIndex):
                        new_groups.append(group)
                    else:
                        new_groups.append(Group(group, Segment(fIndex, sIndex)))
                        add_additional_groups = True
                    if add_additional_groups:
                        while True:
                            sIndex = second.find(fChar, sIndex + 1)
                            if sIndex == -1:
                                break
                            new_groups.append(Group(group, Segment(fIndex, sIndex)))
                else:
                    raise NotImplementedError()
            LineDiff.remove_lowest_groups(new_groups)
            groups = new_groups
        return groups[0]

    @staticmethod
    def remove_lowest_groups(groups):
        maxlength = max(map(lambda group: group.top().length, groups))
        remove_if(groups, lambda group: group.top().length < maxlength)

    def __repr__(self):
        return str(self.group)


class Replacement:
    """
    Class representing a replacement of text upon an original string.

    Members:
    - range (range): A range describing the range of text to replace. A length of zero means we are adding text to the original.
    - text (str): The new text. The empty string means we are removing chars from the original.
    """

    def __init__(self, range, text):
        self.range = range
        self.text = text

    def __repr__(self):
        return str(self.range) + " -> " + self.text


class Replacements:
    def __init__(self, first, second):
        self.replacements = []
        diff = LineDiff(first, second)
        segments = diff.group.get_segments()
        prev_segment = segments[0]
        for segment in islice(segments, 1, len(segments)):
            old_range = range(prev_segment.fIndex + 1, prev_segment.fIndex + 1)
            new_text = second[prev_segment.sIndex + 1 : segment.sIndex - segment.length + 1]
            self.replacements.append(Replacement(old_range, new_text))
            prev_segment = segment

    def __repr__(self):
        return str(self.replacements)

def remove_if(array, predicate):
    """
    removes elements from array in place that match the given predicate
    running time O(N)
    """
    i = 0
    for i, elem in enumerate(array):
        if predicate(elem):
            __remove_if_internal(array, predicate, i + 1)
    return

def __remove_if_internal(array, predicate, i):
    offset = 1
    length = len(array)
    while i < length:
        if predicate(array[i]):
            offset += 1
        else:
            array[i - offset] = array[i]
        i += 1
    del array[length - offset : length]

