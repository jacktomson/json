#! /usr/bin/env python

import heapq
import sys
import tempfile
import unittest


# Object that holds the payload data.
class Record(object):

  def __init__(self, key=None, value=None, stream=None):
    self.key = key
    self.value = value  # not used yet, just for illustration
    self.stream = stream  # intrusive field

  def __lt__(self, other):
    return self.key < other.key or (self.key == other.key and
                                    self.value < other.value)

  def __eq__(self, other):
    return self.key == other.key and self.value == other.value

  def swap(self, other):
    tmp = other.key, other.value
    other.key, other.value = self.key, self.value
    self.key, self.value = tmp

  def serializeToStream(self, stream):
    stream.write(self.key)
    stream.write('\t')
    stream.write(self.value)
    stream.write('\n')

  def deserializeFromStream(self):
    line = self.stream.readline()
    if not line:
      return False
    fields = line.rstrip('\n').split('\t')
    assert len(fields) == 2
    self.key, self.value = fields
    return True


def DeserializeNewRecordFromStream(stream):
  record = Record(None, None, stream)
  if record.deserializeFromStream():
    return record
  else:
    return None


class Sorter(object):
  """Performs an external merge sort.
  Usage example:

  sorter = Sorter(100)
  # First, add some data.
  while someCondition:
    soter.add(getSomeData())

  # Second, "sort".
  sorter.sort()

  # Third, iterate over the data in sorted order.
  record = Record()
  while sorter.hasNext():
    sorter.getNext(record)
    doStuffWith(record)
  # Optionally reset the sorter to use it again.
  sorter.reset()
  """

  def __init__(self, max_buffer_size):
    self.max_buffer_size = max_buffer_size
    self.appendable = True
    self.buffer = PriorityQueue(max_buffer_size)
    self.files = []

  def reset(self):
    for f in self.files:
      f.close()
    del self.files[:]
    self.buffer.clear()
    self.appendable = True

  def writeBufferToTempFile(self):
    if self.buffer.size() == 0:
      return
    f = tempfile.NamedTemporaryFile()
    self.files.append(f)
    # self.buffer is a min-heap, so this is Heapsort:
    while self.buffer.size() > 0:
      record = self.buffer.poll()
      record.serializeToStream(f)
    f.flush()

  def append(self, record):
    assert self.appendable
    assert record is not None
    self.buffer.add(record)
    if self.buffer.size() >= self.max_buffer_size:
      self.writeBufferToTempFile()

  def sort(self):
    """Does not actually sort. Just writes the last buffer to a file
    and prepares the object for reading from and merging the files."""
    assert self.appendable
    self.appendable = False
    self.writeBufferToTempFile()
    assert self.buffer.size() == 0
    # ShowFiles(self.files)  # debugging only
    for file in self.files:
      file.seek(0)
      record = DeserializeNewRecordFromStream(file)
      if record is not None:
        self.buffer.add(record)

  # Java-style iteration (hasNext/next) over sorted elements, interleaving
  # the external files. This is Mergesort.

  def hasNext(self):
    assert not self.appendable
    return self.buffer.peek() is not None

  def next(self):
    """Returns the next record as a newly allocated object."""
    record = Record()
    self.getNext(record)
    return record

  def getNext(self, record_out):
    """Copies the contents of the next record into record_out. Does not
    allocate any new records."""
    assert self.hasNext()
    record = self.buffer.poll()
    record.swap(record_out)
    if record.deserializeFromStream():
      self.buffer.add(record)


# EOF
###############################################################################
# Debugging, boilerplate, tests etc. below

# Debugging only
def ShowFiles(files):
  for i, f in enumerate(files):
    f.seek(0)
    sys.stderr.write('Listing file %d (%s):\n' % (i, f.name))
    while True:
      line = f.readline()
      if not line:
        break
      sys.stderr.write('  %s' % line)
  sys.stderr.flush()


# Use Python's heapq to simulate a Java PriorityQueue:
class PriorityQueue(object):

  def __init__(self, initial_capacity=None):
    self.pq = []

  def add(self, elem):
    heapq.heappush(self.pq, elem)
    return True

  def clear(self):
    del self.pq[:]

  def peek(self):
    if self.pq:
      return self.pq[0]
    else:
      return None

  def poll(self):
    if self.pq:
      return heapq.heappop(self.pq)
    else:
      return None

  def size(self):
    return len(self.pq)


class SorterTest(unittest.TestCase):

  def testSorter(self):
    sorter = Sorter(2)
    for _ in range(3):
      data = [
        Record('7', 'seven'),
        Record('3', 'three'),
        Record('8', 'eight'),
        Record('1', 'one'),
        Record('7', 'seven 2'),
        Record('2', 'two'),
        Record('7', 'seven 3'),
      ]
      sorter = Sorter(2)
      for record in data:
        sorter.append(record)
      sorter.sort()

      data.sort(key = lambda r: r.key)

      i = 0
      record = Record()
      while sorter.hasNext():
        sorter.getNext(record)
        self.assertEqual(record, data[i])
        i += 1
      self.assertEqual(i, len(data))
      sorter.reset()


if __name__ == '__main__':
  sorter = Sorter(2)
  while True:
    record = DeserializeNewRecordFromStream(sys.stdin)
    if not record:
      break
    sorter.append(record)
  sorter.sort()
  record = Record()
  while sorter.hasNext():
    sorter.getNext(record)
    sys.stdout.write('key: "%s" value: "%s"\n' %
                     (record.key, record.value))
