# Lint as: python2, python3
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utilities for monitoring of TFX components and pipelines."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
from typing import List, Text

from tfx import version


class JobLabels(object):
  """A collection of labels for job-style integration.

  Labels are common used in container based systems (see [1] for docker and [2]
  for Kubernetes)
  as well as Google Cloud [3]. This is generally free-from key-value pairs.

  Because different labels are often generated at different stage of pipeline
  execution but only gathered and sent to external intergration when submitting
  a job, we allow adding labels in the following mechanisms (TBA)

  [1] https://docs.docker.com/config/labels-custom-metadata/

  """

  @staticmethod
  def generate() -> 'JobLabels':
    """Generate job labels from current process.

    Returns:
      A set of labels to forward to the underlying jobs
    """
    return JobLabels()


def make_beam_label_args(beam_pipeline_args: List[Text]) -> List[Text]:
  """Make Beam arguments for common labels used in TFX pipelines.

  Args:
    beam_pipeline_args: original Beam pipeline args.

  Returns:
    updated Beam pipeline args with TFX dependencies added.
  """
  labels_kv = {
      'tfx_version': version.__version__,
      'py_version': '%d.%d' % (sys.version_info.major, sys.version_info.minor),
  }
  # See following file for reference to the '--labes ' flag.
  # https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/pipeline_options.py
  labels_flags = ['--labels %s=%s' % (k, v) for k, v in labels_kv.items()]
  return beam_pipeline_args + labels_flags
