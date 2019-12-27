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
"""Commands for copy_template."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Text

import click

from tfx.tools.cli import labels
from tfx.tools.cli.cli_context import Context
from tfx.tools.cli.cli_context import pass_context
from tfx.tools.cli.handler.template_handler import TemplateHandler


@click.group('template')
def template_group() -> None:
  pass


@template_group.command('list', help='List available templates')
def list_templates() -> None:
  click.echo('Available templates:')
  for model in TemplateHandler().list():
    click.echo('- {}'.format(model))


@template_group.command('copy', help='Copy a template to destination directory')
@pass_context
@click.option(
    '--pipeline_name', required=True, type=str, help='Name of the pipeline')
@click.option(
    '--destination_path',
    required=True,
    type=str,
    help='Destination directory path to copy the pipeline template')
@click.option(
    '--model',
    required=True,
    type=str,
    help='Name of the template to copy. Currently, `classification` is the only template provided.'
)
def copy(ctx: Context, pipeline_name: Text, destination_path: Text,
         model: Text) -> None:
  """Command definition to copy template to specified directory."""
  click.echo('Copying {} pipeline template'.format(model))
  ctx.flags_dict[labels.PIPELINE_NAME] = pipeline_name
  ctx.flags_dict[labels.DESTINATION_PATH] = destination_path
  ctx.flags_dict[labels.MODEL] = model
  TemplateHandler().copy(ctx.flags_dict)