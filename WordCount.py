from __future__ import absolute_import
from __future__ import print_function
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
with beam.Pipeline(options=PipelineOptions()) as p:
	lines = p | 'read' >> beam.io.ReadFromText('gs://tegclorox/Input/test_word.txt')
	lines1 = lines | 'tokenize' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
	lines2 = lines1 | 'combine' >> beam.combiners.Count.PerElement()
	lines3 = lines2 | 'key_value' >> beam.Map(lambda word_count: '%s: %s' % (word_count[0], word_count[1]))
	lines4 = lines3 | 'output' >> beam.io.WriteToText('gs://tegclorox/Output/op1.txt')
