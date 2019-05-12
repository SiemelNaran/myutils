#!/usr/bin/python3

class Segment:
    def __init__(self, fIndex, sIndex):
        self.length = 1 if fIndex >= 0 else 0
        self.fIndex = fIndex
        self.sIndex = sIndex

class Group:
    def __init__(self, segment):
        self.segments = [segment]

    def top(self):
        return self.segments[-1]

    def add_segment(self, segment):
        top = self.top()
        if top.fIndex + 1 == segment.fIndex and top.sIndex + 1 == segment.sIndex:
            top.len += 1
            top.fIndex += 1
            top.sIndex += 1
        else:
            self.segments.append(segment)

# getone
# getnowone

class LineDiff:
    def __init__(self, first, second):
        self.group = compute_group(first, second)

    def compute_group(first, second):
        groups = [Group(Segment(-1, -1))]
        for fIndex, fChar in enumerate(first):
            for group in Groups:
                segment = group.top()
                sIndex = second.find(fChar, segment.sIndex + 1)
                group.add_segment(Segment(fIndex, sIndex))
            remove_lowest_groups(groups)
        return groups[0]

    def remove_lowest_groups(groups):
        maxlength = max(map(lambda group: group.top().length, groups))
        for index in range(len(groups) - 1, -1, -1):
            group = groups[index]
            if group.top().length < maxlength:
                del group[index]
        return list(filter(lambda group: group.top().length == maxlength, groups))


def remove_if(array, predicate):
    """
    removes elements from array that match the given predicate
    """
    length = len(array)
    i = 0
    while i < length:
        if predicate(array[i]):
            while True:
                start = i
                for i in range(i + 1, length):
                    if not predicate(array[i]):
                        break
                offset += i - start
                for i in range(i + 1, length):
                    if not predicate(array[i]):
                        array[i - offset] = array[i]
            del array[length - offset : length]
            i += 1
            break
        i += 1
    offset = 0


