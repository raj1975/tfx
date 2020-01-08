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
"""TFX ml metadata library."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import copy
import hashlib
import os
import random
import time
import types
from typing import Any, Dict, List, Optional, Text, Type, Union

import absl
import six
import tensorflow as tf

from ml_metadata.metadata_store import metadata_store
from ml_metadata.proto import metadata_store_pb2
from tensorflow.python.lib.io import file_io  # pylint: disable=g-direct-tensorflow-import
from tfx.orchestration import data_types
from tfx.types.artifact import Artifact
from tfx.types.artifact import ArtifactState

# Number of times to retry initialization of connection.
_MAX_INIT_RETRY = 10

# Maximum number of executions we look at for previous result.
MAX_EXECUTIONS_FOR_CACHE = 100
# Execution state constant. We should replace this with MLMD enum once that is
# ready.
EXECUTION_STATE_CACHED = 'cached'
EXECUTION_STATE_COMPLETE = 'complete'
EXECUTION_STATE_NEW = 'new'
# Context type, the following three types of contexts are supported:
#  - pipeline level context is shared within one pipeline, across multiple
#    pipeline runs.
#  - pipeline run level context is shared within one pipeline run, across
#    all component executions in that pipeline run.
#  - component run level context is shared within one component run.
_CONTEXT_TYPE_PIPELINE = 'pipeline'
_CONTEXT_TYPE_PIPELINE_RUN = 'run'
_CONTEXT_TYPE_COMPONENT_RUN = 'component_run'
# Keys of context type properties.
_CONTEXT_TYPE_KEY_PIPELINE_NAME = 'pipeline_name'
_CONTEXT_TYPE_KEY_RUN_ID = 'run_id'
_CONTEXT_TYPE_KEY_COMPONENT_ID = 'component_id'
# Keys of execution type properties.
_EXECUTION_TYPE_KEY_CHECKSUM = 'checksum_md5'
_EXECUTION_TYPE_KEY_PIPELINE_NAME = 'pipeline_name'
_EXECUTION_TYPE_KEY_PIPELINE_ROOT = 'pipeline_root'
_EXECUTION_TYPE_KEY_RUN_ID = 'run_id'
_EXECUTION_TYPE_KEY_COMPONENT_ID = 'component_id'
_EXECUTION_TYPE_RESERVED_KEYS = {
    _EXECUTION_TYPE_KEY_CHECKSUM, _EXECUTION_TYPE_KEY_PIPELINE_NAME,
    _EXECUTION_TYPE_KEY_PIPELINE_ROOT, _EXECUTION_TYPE_KEY_RUN_ID,
    _EXECUTION_TYPE_KEY_COMPONENT_ID
}


def sqlite_metadata_connection_config(
    metadata_db_uri: Text) -> metadata_store_pb2.ConnectionConfig:
  """Convenience function to create file based metadata connection config.

  Args:
    metadata_db_uri: uri to metadata db.

  Returns:
    A metadata_store_pb2.ConnectionConfig based on given metadata db uri.
  """
  tf.io.gfile.makedirs(os.path.dirname(metadata_db_uri))
  connection_config = metadata_store_pb2.ConnectionConfig()
  connection_config.sqlite.filename_uri = metadata_db_uri
  connection_config.sqlite.connection_mode = \
    metadata_store_pb2.SqliteMetadataSourceConfig.READWRITE_OPENCREATE
  return connection_config


def mysql_metadata_connection_config(
    host: Text, port: int, database: Text, username: Text,
    password: Text) -> metadata_store_pb2.ConnectionConfig:
  """Convenience function to create mysql-based metadata connection config.

  Args:
    host: The name or network address of the instance of MySQL to connect to.
    port: The port MySQL is using to listen for connections.
    database: The name of the database to use.
    username: The MySQL login account being used.
    password: The password for the MySQL account being used.

  Returns:
    A metadata_store_pb2.ConnectionConfig based on given metadata db uri.
  """
  return metadata_store_pb2.ConnectionConfig(
      mysql=metadata_store_pb2.MySQLDatabaseConfig(
          host=host,
          port=port,
          database=database,
          user=username,
          password=password))


# TODO(ruoyu): Figure out the story mutable UDFs. We should not reuse previous
# run when having different UDFs.
class Metadata(object):
  """Helper class to handle metadata I/O."""

  def __init__(self,
               connection_config: metadata_store_pb2.ConnectionConfig) -> None:
    self._connection_config = connection_config
    self._store = None

  def __enter__(self) -> 'Metadata':
    # TODO(ruoyu): Establishing a connection pool instead of newing
    # a connection every time. Until then, check self._store before usage
    # in every method.
    for _ in range(_MAX_INIT_RETRY):
      try:
        self._store = metadata_store.MetadataStore(self._connection_config)
      except RuntimeError:
        # MetadataStore could raise Aborted error if multiple concurrent
        # connections try to execute initialization DDL in database.
        # This is safe to retry.
        time.sleep(random.random())
        continue
      else:
        return self

    raise RuntimeError('Failed to establish connection to Metadata storage.')

  def __exit__(self, exc_type: Optional[Type[Exception]],
               exc_value: Optional[Exception],
               exc_tb: Optional[types.TracebackType]) -> None:
    self._store = None

  @property
  def store(self) -> metadata_store.MetadataStore:
    """Returns underlying MetadataStore.

    Raises:
      RuntimeError: if this instance is not in enter state.
    """
    if self._store is None:
      raise RuntimeError('Metadata object is not in enter state')
    return self._store

  def _prepare_artifact_type(
      self, artifact_type: metadata_store_pb2.ArtifactType
  ) -> metadata_store_pb2.ArtifactType:
    if artifact_type.id:
      return artifact_type
    type_id = self._store.put_artifact_type(
        artifact_type=artifact_type, can_add_fields=True)
    artifact_type.id = type_id
    return artifact_type

  def update_artifact_state(self, artifact: metadata_store_pb2.Artifact,
                            new_state: Text) -> None:
    """Update the state of a given artifact."""
    if not artifact.id:
      raise ValueError('Artifact id missing for %s' % artifact)
    # TODO(b/146936257): unify artifact access logic by wrapping raw MLMD
    # artifact protos into tfx.types.Artifact objects at a lower level.
    if 'state' in artifact.properties:
      artifact.properties['state'].string_value = new_state
    else:
      artifact.custom_properties['state'].string_value = new_state
    self._store.put_artifacts([artifact])

  def _upsert_artifacts(self, tfx_artifact_list: List[Artifact],
                        state: Text) -> None:
    """Updates or inserts a list of artifacts.

    This call will also update original tfx artifact list to contain the
    artifact type info and artifact id.

    Args:
      tfx_artifact_list: A list of tfx.types.Artifact. This will be updated with
        MLMD artifact type info and MLMD artifact id.
      state: the artifact state to set.
    """
    for raw_artifact in tfx_artifact_list:
      if not raw_artifact.type_id:
        artifact_type = self._prepare_artifact_type(raw_artifact.artifact_type)
        raw_artifact.set_mlmd_artifact_type(artifact_type)
      raw_artifact.state = state
    artifact_ids = self.store.put_artifacts(
        [x.mlmd_artifact for x in tfx_artifact_list])
    for a, aid in zip(tfx_artifact_list, artifact_ids):
      a.id = aid

  def publish_artifacts(self, tfx_artifact_list: List[Artifact]) -> None:
    """Publish artifacts to MLMD.

    This call will also update original tfx artifact list to contain the
    artifact type info and artifact id.

    Args:
      tfx_artifact_list: A list of tfx.types.Artifact. This will be updated
    """
    self._upsert_artifacts(tfx_artifact_list, ArtifactState.PUBLISHED)

  def get_all_artifacts(self) -> List[metadata_store_pb2.Artifact]:
    try:
      return self._store.get_artifacts()
    except tf.errors.NotFoundError:
      return []

  def get_artifacts_by_uri(self,
                           uri: Text) -> List[metadata_store_pb2.Artifact]:
    try:
      return self._store.get_artifacts_by_uri(uri)
    except tf.errors.NotFoundError:
      return []

  def get_artifacts_by_type(
      self, type_name: Text) -> List[metadata_store_pb2.Artifact]:
    try:
      return self._store.get_artifacts_by_type(type_name)
    except tf.errors.NotFoundError:
      return []

  def _prepare_event(self, execution_id: int, artifact_id: int, key: Text,
                     index: int, event_type: Any) -> metadata_store_pb2.Event:
    """Commits a single event to the repository."""
    event = metadata_store_pb2.Event()
    event.artifact_id = artifact_id
    event.execution_id = execution_id
    step = event.path.steps.add()
    step.key = key
    step = event.path.steps.add()
    step.index = index
    event.type = event_type
    return event

  # TODO(b/143081379): We might need to revisit schema evolution story.
  def _prepare_execution_type(self, type_name: Text,
                              exec_properties: Dict[Text, Any]) -> int:
    """Get a execution type.

    Uses existing type if schema is superset of what is needed. Otherwise tries
    to register new execution type.

    Args:
      type_name: the name of the execution type
      exec_properties: the execution properties included by the execution

    Returns:
      execution type id
    Raises:
      ValueError if new execution type conflicts with existing schema in MLMD.
    """
    try:
      existing_execution_type = self._store.get_execution_type(type_name)
      if existing_execution_type is None:
        raise RuntimeError('Execution type is None for %s.' % type_name)
      if all(k in existing_execution_type.properties
             for k in exec_properties.keys()):
        return existing_execution_type.id
      else:
        raise tf.errors.NotFoundError(None, None,
                                      'No qualified execution type found.')
    except tf.errors.NotFoundError:
      execution_type = metadata_store_pb2.ExecutionType(name=type_name)
      execution_type.properties['state'] = metadata_store_pb2.STRING
      # If exec_properties contains new entries, execution type schema will be
      # updated in MLMD.
      for k in exec_properties.keys():
        assert k not in _EXECUTION_TYPE_RESERVED_KEYS, (
            'execution properties with reserved key %s') % k
        execution_type.properties[k] = metadata_store_pb2.STRING
      # TODO(ruoyu): Find a better place / solution to the checksum logic.
      if 'module_file' in exec_properties:
        execution_type.properties[
            _EXECUTION_TYPE_KEY_CHECKSUM] = metadata_store_pb2.STRING
      execution_type.properties[
          _EXECUTION_TYPE_KEY_PIPELINE_NAME] = metadata_store_pb2.STRING
      execution_type.properties[
          _EXECUTION_TYPE_KEY_PIPELINE_ROOT] = metadata_store_pb2.STRING
      execution_type.properties[
          _EXECUTION_TYPE_KEY_RUN_ID] = metadata_store_pb2.STRING
      execution_type.properties[
          _EXECUTION_TYPE_KEY_COMPONENT_ID] = metadata_store_pb2.STRING

      try:
        execution_type_id = self._store.put_execution_type(
            execution_type=execution_type, can_add_fields=True)
        absl.logging.info('Registering a new execution type with id %s.' %
                          execution_type_id)
        return execution_type_id
      except tf.errors.AlreadyExistsError:
        warning_str = (
            'missing or modified key in exec_properties comparing with '
            'existing execution type with the same type name. Existing type: '
            '%s, New type: %s') % (existing_execution_type, execution_type)
        absl.logging.warning(warning_str)
        raise ValueError(warning_str)

  # TODO(ruoyu): Make pipeline_info and component_info required once migration
  # to go/tfx-oss-artifact-passing finishes.
  def _prepare_execution(
      self,
      state: Text,
      exec_properties: Dict[Text, Any],
      pipeline_info: data_types.PipelineInfo,
      component_info: data_types.ComponentInfo,
  ) -> metadata_store_pb2.Execution:
    """Create a new execution with given type and state."""
    execution = metadata_store_pb2.Execution()
    execution.type_id = self._prepare_execution_type(
        component_info.component_type, exec_properties)
    execution.properties['state'].string_value = tf.compat.as_text(state)
    for k, v in exec_properties.items():
      # We always convert execution properties to unicode.
      execution.properties[k].string_value = tf.compat.as_text(
          tf.compat.as_str_any(v))
    # We also need to checksum UDF file to identify different binary being
    # used. Do we have a better way to checksum a file than hashlib.md5?
    # TODO(ruoyu): Find a better place / solution to the checksum logic.
    # TODO(ruoyu): SHA instead of MD5.
    if 'module_file' in exec_properties and exec_properties[
        'module_file'] and tf.io.gfile.exists(exec_properties['module_file']):
      contents = file_io.read_file_to_string(exec_properties['module_file'])
      execution.properties['checksum_md5'].string_value = tf.compat.as_text(
          tf.compat.as_str_any(
              hashlib.md5(tf.compat.as_bytes(contents)).hexdigest()))
    if pipeline_info:
      execution.properties[
          'pipeline_name'].string_value = pipeline_info.pipeline_name
      execution.properties[
          'pipeline_root'].string_value = pipeline_info.pipeline_root
      if pipeline_info.run_id:
        execution.properties['run_id'].string_value = pipeline_info.run_id
    if component_info:
      execution.properties[
          'component_id'].string_value = component_info.component_id
    absl.logging.debug('Prepared EXECUTION:\n {}'.format(execution))
    return execution

  def _update_execution_state(self, execution: metadata_store_pb2.Execution,
                              new_state: Text) -> None:
    execution.properties['state'].string_value = tf.compat.as_text(new_state)
    self._store.put_executions([execution])

  def register_execution(
      self,
      exec_properties: Dict[Text, Any],
      pipeline_info: data_types.PipelineInfo,
      component_info: data_types.ComponentInfo,
      contexts: Optional[List[metadata_store_pb2.Context]] = None) -> int:
    """Create a new execution in metadata.

    Args:
      exec_properties: the execution properties of the execution.
      pipeline_info: optional pipeline info of the execution.
      component_info: optional component info of the execution.
      contexts: contexts for current run, link it with execution if provided.

    Returns:
      execution id of the new execution.
    """
    execution = self._prepare_execution(EXECUTION_STATE_NEW, exec_properties,
                                        pipeline_info, component_info)
    execution_id, _, _ = self._store.put_execution(execution, [], contexts or
                                                   [])

    return execution_id

  def publish_execution(
      self,
      execution_id: int,
      input_dict: Dict[Text, List[Artifact]],
      output_dict: Dict[Text, List[Artifact]],
      state: Optional[Text] = EXECUTION_STATE_COMPLETE,
  ) -> Dict[Text, List[Artifact]]:
    """Publish an execution with input and output artifacts info.

    Args:
      execution_id: id of execution to be published.
      input_dict: inputs artifacts used by the execution with id ready.
      output_dict: output artifacts produced by the execution without id.
      state: optional state of the execution, default to be
        EXECUTION_STATE_COMPLETE.

    Returns:
      Updated outputs with artifact ids.

    Raises:
      RuntimeError: If any of the following happens:
        1. Execution state not valid for publish
        2. Input artifact id missing
        3. Output artifact id missing when execution state is
           EXECUTION_STATE_CASHED
    """
    if state not in [EXECUTION_STATE_CACHED, EXECUTION_STATE_COMPLETE]:
      raise RuntimeError('Cannot publish execution with state: %s' % state)

    events = []
    if input_dict:
      for key, input_list in input_dict.items():
        for index, single_input in enumerate(input_list):
          if not single_input.mlmd_artifact.id:
            raise RuntimeError('input artifact %s has missing id' %
                               single_input)
          events.append(
              self._prepare_event(
                  execution_id=execution_id,
                  artifact_id=single_input.mlmd_artifact.id,
                  key=key,
                  index=index,
                  event_type=metadata_store_pb2.Event.INPUT))
    if output_dict:
      for key, output_list in output_dict.items():
        for index, single_output in enumerate(output_list):
          if not single_output.mlmd_artifact.id:
            if state == EXECUTION_STATE_CACHED:
              raise RuntimeError(
                  'output artifact id not available for cached output: %s' %
                  single_output)
            self.publish_artifacts([single_output])

          events.append(
              self._prepare_event(
                  execution_id=execution_id,
                  artifact_id=single_output.mlmd_artifact.id,
                  key=key,
                  index=index,
                  event_type=metadata_store_pb2.Event.OUTPUT))

    [execution] = self._store.get_executions_by_id([execution_id])
    self._update_execution_state(execution, state)
    if events:
      self._store.put_events(events)
    absl.logging.debug(
        'Publishing execution %s, with inputs %s and outputs %s' %
        (execution, input_dict, output_dict))
    return output_dict

  def _get_cached_execution_id(
      self, input_dict: Dict[Text, List[Artifact]],
      candidate_execution_ids: List[int]) -> Optional[int]:
    """Gets common execution ids that are related to all the artifacts in input.

    Args:
      input_dict: input used by a component run.
      candidate_execution_ids: a list of id of candidate execution.

    Returns:
      a qualified execution id or None.

    """
    input_ids = set()
    for input_list in input_dict.values():
      for single_input in input_list:
        input_ids.add(single_input.mlmd_artifact.id)

    for execution_id in candidate_execution_ids:
      events = self._store.get_events_by_execution_ids([execution_id])
      execution_input_ids = set([
          event.artifact_id for event in events if event.type in [
              metadata_store_pb2.Event.INPUT,
              metadata_store_pb2.Event.DECLARED_INPUT
          ]
      ])
      if input_ids == execution_input_ids:
        absl.logging.debug(
            'Found matching execution with all input artifacts: %s' %
            execution_id)
        return execution_id
      else:
        absl.logging.debug(
            'Execution %d does not match desired input artifacts', execution_id)
    absl.logging.debug(
        'No execution matching type id and input artifacts found')
    return None

  def _is_eligible_previous_execution(
      self, currrent_execution: metadata_store_pb2.Execution,
      target_execution: metadata_store_pb2.Execution) -> bool:
    currrent_execution.properties['run_id'].string_value = ''
    target_execution.properties['run_id'].string_value = ''
    currrent_execution.id = target_execution.id
    return currrent_execution == target_execution

  # TODO(ruoyu): Leverage artifact-context attribution to enhance performance
  # once publishing execution adopts new pattern.
  def previous_execution(
      self, input_artifacts: Dict[Text, List[Artifact]],
      exec_properties: Dict[Text, Any], pipeline_info: data_types.PipelineInfo,
      component_info: data_types.ComponentInfo) -> Optional[int]:
    """Gets eligible previous execution that takes the same inputs.

    An eligible execution should take the same inputs, execution properties and
    with the same pipeline and component properties.

    Args:
      input_artifacts: inputs used by the run.
      exec_properties: execution properties used by the run.
      pipeline_info: info of the current pipeline run.
      component_info: info of the current component.

    Returns:
      Execution id of previous run that takes the input dict. None if not found.
    """
    absl.logging.debug(
        'Checking previous run for execution_type_name %s and input_artifacts %s',
        component_info.component_type, input_artifacts)

    # Ids of candidate executions which share the same execution property as
    # current.
    candidate_execution_ids = []
    expected_previous_execution = self._prepare_execution(
        EXECUTION_STATE_COMPLETE,
        exec_properties,
        pipeline_info=pipeline_info,
        component_info=component_info)
    context = self.get_pipeline_context(pipeline_info)
    if context is None:
      absl.logging.warning('Pipeline context not available for %s' %
                           pipeline_info)
      return None
    for execution in self._store.get_executions_by_context(context.id):
      if self._is_eligible_previous_execution(
          copy.deepcopy(expected_previous_execution), copy.deepcopy(execution)):
        candidate_execution_ids.append(execution.id)
    candidate_execution_ids.sort(reverse=True)
    candidate_execution_ids = candidate_execution_ids[
        0:min(len(candidate_execution_ids), MAX_EXECUTIONS_FOR_CACHE)]

    return self._get_cached_execution_id(input_artifacts,
                                         candidate_execution_ids)

  # TODO(b/136031301): This should be merged with previous_run.
  def fetch_previous_result_artifacts(
      self, output_dict: Dict[Text, List[Artifact]],
      execution_id: int) -> Dict[Text, List[Artifact]]:
    """Fetches output with artifact ids produced by a previous run.

    Args:
      output_dict: a dict from name to a list of output Artifact objects.
      execution_id: the id of the execution that produced the outputs.

    Returns:
      Original output_dict with artifact id inserted.

    Raises:
      RuntimeError: path change without clean metadata.
    """

    name_to_index_to_artifacts = collections.defaultdict(dict)
    for event in self._store.get_events_by_execution_ids([execution_id]):
      if event.type == metadata_store_pb2.Event.OUTPUT:
        [artifact] = self._store.get_artifacts_by_id([event.artifact_id])
        output_key = event.path.steps[0].key
        output_index = event.path.steps[1].index
        name_to_index_to_artifacts[output_key][output_index] = artifact
    for output_name, output_list in output_dict.items():
      if output_name not in name_to_index_to_artifacts:
        raise RuntimeError('Unmatched output name from previous execution.')
      index_to_artifacts = name_to_index_to_artifacts[output_name]
      if len(output_list) != len(index_to_artifacts):
        raise RuntimeError('Output name expected %s items but %s retrieved' %
                           (len(output_list), len(index_to_artifacts)))
      for index, output in enumerate(output_list):
        output.set_mlmd_artifact(index_to_artifacts[index])
    return dict(output_dict)

  def search_artifacts(self, artifact_name: Text,
                       pipeline_info: data_types.PipelineInfo,
                       producer_component_id: Text) -> List[Artifact]:
    """Search artifacts that matches given info.

    Args:
      artifact_name: the name of the artifact that set by producer component.
        The name is logged both in artifacts and the events when the execution
        being published.
      pipeline_info: the information of the current pipeline
      producer_component_id: the id of the component that produces the artifact

    Returns:
      A list of Artifacts that matches the given info

    Raises:
      RuntimeError: when no matching execution is found given producer info.
    """
    producer_execution = None
    matching_artifact_ids = set()
    # TODO(ruoyu): We need to revisit this when adding support for async
    # execution.
    context = self.get_pipeline_run_context(pipeline_info)
    if context is None:
      raise RuntimeError('Pipeline run context for %s does not exist' %
                         pipeline_info)
    for execution in self._store.get_executions_by_context(context.id):
      if execution.properties[
          'component_id'].string_value == producer_component_id:
        producer_execution = execution
        break
    if not producer_execution:
      raise RuntimeError('Cannot find matching execution with pipeline name %s,'
                         'run id %s and component id %s' %
                         (pipeline_info.pipeline_name, pipeline_info.run_id,
                          producer_component_id))
    for event in self._store.get_events_by_execution_ids(
        [producer_execution.id]):
      if (event.type == metadata_store_pb2.Event.OUTPUT and
          event.path.steps[0].key == artifact_name):
        matching_artifact_ids.add(event.artifact_id)

    # Get relevant artifacts along with their types.
    artifacts_by_id = self._store.get_artifacts_by_id(
        list(matching_artifact_ids))
    matching_artifact_type_ids = list(set(a.type_id for a in artifacts_by_id))
    matching_artifact_types = self._store.get_artifact_types_by_id(
        matching_artifact_type_ids)
    artifact_types = dict(
        zip(matching_artifact_type_ids, matching_artifact_types))

    result_artifacts = []
    for a in artifacts_by_id:
      tfx_artifact = Artifact(mlmd_artifact_type=artifact_types[a.type_id])
      tfx_artifact.set_mlmd_artifact(a)
      tfx_artifact.set_mlmd_artifact_type(artifact_types[a.type_id])
      result_artifacts.append(tfx_artifact)
    return result_artifacts

  def get_all_runs(self, pipeline_name: Text):
    """Get all runs for a given pipeline name.

    Args:
      pipeline_name: name of the pipeline.

    Returns:
      A List of run id.
    """
    result = []
    # TODO(b/139092990): support get_contexts_by_property.
    for context in self._store.get_contexts_by_type(_CONTEXT_TYPE_PIPELINE_RUN):
      if context.properties['pipeline_name'].string_value == pipeline_name:
        result.append(context.properties['run_id'].string_value)
    return result

  def get_execution_states(self, pipeline_info: data_types.PipelineInfo):
    """Get components execution states for a given pipeline.

    Args:
      pipeline_info: target pipeline's information.

    Returns:
      A Dict of component id to its state mapping.
    """
    pipeline_run_context = self._store.get_context_by_type_and_name(
        _CONTEXT_TYPE_PIPELINE_RUN, pipeline_info.pipeline_run_context_name)
    result = {}
    if not pipeline_run_context:
      return result
    for execution in self._store.get_executions_by_context(
        pipeline_run_context.id):
      result[execution.properties['component_id']
             .string_value] = execution.properties['state'].string_value
    return result

  def _register_context_type_if_not_exist(
      self, context_type_name: Text,
      properties: Dict[Text, 'metadata_store_pb2.PropertyType']) -> int:
    """Registers a context type if not exist, otherwise returns existing one.

    Args:
      context_type_name: the name of the context.
      properties: properties of the context.

    Returns:
      id of the desired context type.
    """
    context_type = metadata_store_pb2.ContextType(name=context_type_name)
    for k, t in properties.items():
      context_type.properties[k] = t
    context_type_id = self._store.put_context_type(
        context_type, can_add_fields=True)

    return context_type_id

  def _register_context_if_not_exist(
      self, context_type_name: Text, context_name: Text,
      properties: Dict[Text, Union[int, float, Text]]
  ) -> metadata_store_pb2.Context:
    """Registers a context if not exist, otherwise returns the existing one.

    Args:
      context_type_name: the name of the context type desired.
      context_name: the name of the context.
      properties: properties to set in the context.

    Returns:
      id of the desired context

    Raises:
      RuntimeError: when meeting unexpected property type.
    """
    # TODO(ruoyu): Centralize the type definition / mapping along with Artifact
    # property types.
    property_type_mapping = {
        int: metadata_store_pb2.INT,
        six.binary_type: metadata_store_pb2.STRING,
        six.text_type: metadata_store_pb2.STRING,
        float: metadata_store_pb2.DOUBLE
    }
    context_type_id = self._register_context_type_if_not_exist(
        context_type_name,
        dict(
            (k, property_type_mapping[type(k)]) for k, v in properties.items()))

    context = metadata_store_pb2.Context(
        type_id=context_type_id, name=context_name)
    for k, v in properties.items():
      if isinstance(v, int):
        context.properties[k].int_value = v
      elif isinstance(v, six.string_types):
        context.properties[k].string_value = v
      elif isinstance(v, float):
        context.properties[k].double_value = v
      else:
        raise RuntimeError('Unexpected property type: %s' % type(v))
    try:
      [context_id] = self._store.put_contexts([context])
      context.id = context_id
    except tf.errors.AlreadyExistsError:
      absl.logging.debug('Run context %s already exists.', context_name)
      context = self.store.get_context_by_type_and_name(context_type_name,
                                                        context_name)
      assert context is not None, 'Run context is missing for %s.' % (
          context_name)

    absl.logging.debug('ID of run context %s is %s.', context_name, context.id)
    return context

  def get_component_run_context(
      self, component_info: data_types.ComponentInfo
  ) -> Optional[metadata_store_pb2.Context]:
    """Gets the context for the component run.

    Args:
      component_info: component information for the current component run.

    Returns:
      a matched context or None
    """
    return self.store.get_context_by_type_and_name(
        _CONTEXT_TYPE_COMPONENT_RUN, component_info.component_run_context_name)

  def get_pipeline_context(
      self, pipeline_info: data_types.PipelineInfo
  ) -> Optional[metadata_store_pb2.Context]:
    """Gets the context for the pipeline run.

    Args:
      pipeline_info: pipeline information for the current pipeline run.

    Returns:
      a matched context or None
    """
    return self.store.get_context_by_type_and_name(
        _CONTEXT_TYPE_PIPELINE, pipeline_info.pipeline_context_name)

  def get_pipeline_run_context(
      self, pipeline_info: data_types.PipelineInfo
  ) -> Optional[metadata_store_pb2.Context]:
    """Gets the context for the pipeline run.

    Args:
      pipeline_info: pipeline information for the current pipeline run.

    Returns:
      a matched context or None
    """
    if pipeline_info.run_id:
      return self.store.get_context_by_type_and_name(
          _CONTEXT_TYPE_PIPELINE_RUN, pipeline_info.pipeline_run_context_name)
    else:
      return None

  def register_contexts_if_not_exists(
      self, pipeline_info: data_types.PipelineInfo,
      component_info: data_types.ComponentInfo
  ) -> List[metadata_store_pb2.Context]:
    """Creates or fetches the contexts needed for the run.

    There are three potential contexts:
      - Context for the pipeline.
      - Context for the current pipeline run. This is optional, only available
        when run_id is specified.
      - Context for the current component run.

    Args:
      pipeline_info: pipeline information for current run.
      component_info: component information for the current component run.

    Returns:
      a list (of size three) of context.
    """
    # Gets the pipeline level context.
    result = []
    pipeline_context = self._register_context_if_not_exist(
        context_type_name=_CONTEXT_TYPE_PIPELINE,
        context_name=pipeline_info.pipeline_context_name,
        properties={
            _CONTEXT_TYPE_KEY_PIPELINE_NAME: pipeline_info.pipeline_name
        })
    result.append(pipeline_context)
    absl.logging.debug('Pipeline context [%s : %s]',
                       pipeline_info.pipeline_context_name, pipeline_context.id)
    # If run id exists, gets the pipeline run level context.
    if pipeline_info.run_id:
      pipeline_run_context = self._register_context_if_not_exist(
          context_type_name=_CONTEXT_TYPE_PIPELINE_RUN,
          context_name=pipeline_info.pipeline_run_context_name,
          properties={
              _CONTEXT_TYPE_KEY_PIPELINE_NAME: pipeline_info.pipeline_name,
              _CONTEXT_TYPE_KEY_RUN_ID: pipeline_info.run_id
          })
      result.append(pipeline_run_context)
      absl.logging.debug('Pipeline run context [%s : %s]',
                         pipeline_info.pipeline_run_context_name,
                         pipeline_run_context.id)
    # Gets the component run level context.
    component_run_context = self._register_context_if_not_exist(
        context_type_name=_CONTEXT_TYPE_COMPONENT_RUN,
        context_name=component_info.component_run_context_name,
        properties={
            _CONTEXT_TYPE_KEY_PIPELINE_NAME: pipeline_info.pipeline_name,
            _CONTEXT_TYPE_KEY_RUN_ID: pipeline_info.run_id,
            _CONTEXT_TYPE_KEY_COMPONENT_ID: component_info.component_id
        })
    result.append(component_run_context)

    absl.logging.debug('Component run context [%s : %s]',
                       component_info.component_run_context_name,
                       component_run_context.id)
    return result
