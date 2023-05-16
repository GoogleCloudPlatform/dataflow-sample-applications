#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import argparse
import logging
import os
import sys


def run(argv):
  # Import here to avoid __main__ session pickling issues.
  import io
  import itertools
  import matplotlib.pyplot as plt
  import collatz

  import apache_beam as beam
  from apache_beam.io import restriction_trackers
  from apache_beam.options.pipeline_options import PipelineOptions

  class RangeSdf(beam.DoFn, beam.RestrictionProvider):
    """An SDF producing all the integers in the input range.

    This is preferable to beam.Create(range(...)) as it produces the integers
    dynamically rather than materializing them up front.  It is an SDF to do
    so with perfect dynamic sharding.
    """
    def initial_restriction(self, desired_range):
      start, stop = desired_range
      return restriction_trackers.OffsetRange(start, stop)

    def restriction_size(self, _, restriction):
      return restriction.size()

    def create_tracker(self, restriction):
      return restriction_trackers.OffsetRestrictionTracker(restriction)

    def process(self, _, active_range=beam.DoFn.RestrictionParam()):
      for i in itertools.count(active_range.current_restriction().start):
        if active_range.try_claim(i):
          yield i
        else:
          break

  class GenerateIntegers(beam.PTransform):
    def __init__(self, start, stop):
      self._start = start
      self._stop = stop

    def expand(self, p):
      return (
          p
          | beam.Create([(self._start, self._stop + 1)])
          | beam.ParDo(RangeSdf()))

  parser = argparse.ArgumentParser()
  parser.add_argument('--start', dest='start', type=int, default=1)
  parser.add_argument('--stop', dest='stop', type=int, default=10000)
  parser.add_argument('--output', default='./out.png')

  known_args, pipeline_args = parser.parse_known_args(argv)
  # Store this as a local to avoid capturing the full known_args.
  output_path = known_args.output

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

    # Generate the integers from start to stop (inclusive).
    integers = p | GenerateIntegers(known_args.start, known_args.stop)

    # Run them through our C++ function, filtering bad records.
    # Requires apache beam 2.34 or later.
    stopping_times, bad_values = (
        integers
        | beam.Map(collatz.total_stopping_time).with_exception_handling(
            use_subprocess=True))

    # Write the bad values to a side channel.
    bad_values | 'WriteBadValues' >> beam.io.WriteToText(
        os.path.splitext(output_path)[0] + '-bad.txt')

    # Count the occurrence of each stopping time and normalize.
    total = known_args.stop - known_args.start + 1
    frequencies = (
        stopping_times
        | 'Aggregate' >> (beam.Map(lambda x: (x, 1)) | beam.CombinePerKey(sum))
        | 'Normalize' >> beam.MapTuple(lambda x, count: (x, count / total)))

    if known_args.stop <= 10:
      # Print out the results for debugging.
      frequencies | beam.Map(print)
    else:
      # Format and write them to a text file.
      (
          frequencies
          | 'Format' >> beam.MapTuple(lambda count, freq: f'{count}, {freq}')
          | beam.io.WriteToText(os.path.splitext(output_path)[0] + '.txt'))

    # Define some helper functions.
    def make_scatter_plot(xy):
      x, y = zip(*xy)
      plt.plot(x, y, '.')
      png_bytes = io.BytesIO()
      plt.savefig(png_bytes, format='png')
      png_bytes.seek(0)
      return png_bytes.read()

    def write_to_path(path, content):
      """Most Beam IOs write multiple elements to some kind of a container
      file (e.g. strings to lines of a text file, avro records to an avro file,
      etc.)  This function writes each element to its own file, given by path.
      """
      # Write to a temporary path and to a rename for fault tolerence.
      tmp_path = path + '.tmp'
      fs = beam.io.filesystems.FileSystems.get_filesystem(path)
      with fs.create(tmp_path) as fout:
        fout.write(content)
      fs.rename([tmp_path], [path])

    (
        p
        # Create a PCollection with a single element.
        | 'CreateSingleton' >> beam.Create([None])
        # Process the single element with a Map function, passing the frequency
        # PCollection as a side input.
        # This will cause the normally distributed frequency PCollection to be
        # colocated and processed as a single unit, producing a single output.
        | 'MakePlot' >> beam.Map(
            lambda _,
            data: make_scatter_plot(data),
            data=beam.pvalue.AsList(frequencies))
        # Pair this with the desired filename.
        |
        'PairWithFilename' >> beam.Map(lambda content: (output_path, content))
        # And actually write it out, using MapTuple to split the tuple into args.
        | 'WriteToOutput' >> beam.MapTuple(write_to_path))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv)
