#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Apache/airflow/airflow/providers/apache/hive/operators/hive_to_samba.py /
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
This module contains operator to move data from Hive to Samba.
"""

from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars


class Hive2SambaOperator(BaseOperator):
    """
    Executes hql code in a specific Hive database and loads the
    results of the query as a csv to a Samba location.
    :param hql: the hql to be exported. (templated)
    :type hql: str
    :param destination_filepath: the file path to where the file will be pushed onto samba
    :type destination_filepath: str
    :param samba_conn_id: reference to the samba destination
    :type samba_conn_id: str
    :param hiveserver2_conn_id: reference to the hiveserver2 service
    :type hiveserver2_conn_id: str
    """

    template_fields = ('hql', 'destination_filepath')
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(self,
                 hql: str,
                 destination_filepath: str,
                 samba_conn_id: str = 'samba_default',
                 hiveserver2_conn_id: str = 'hiveserver2_default',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.samba_conn_id = samba_conn_id
        self.destination_filepath = destination_filepath
        self.hql = hql.strip().rstrip(';')

    def execute(self, context):
        with NamedTemporaryFile() as tmp_file:
            self.log.info("Fetching file from Hive")
            hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)
            hive.to_csv(hql=self.hql, csv_filepath=tmp_file.name, hive_conf=context_to_airflow_vars(context))
            self.log.info("Pushing to samba")
            samba = SambaHook(samba_conn_id=self.samba_conn_id)
            samba.push_from_local(self.destination_filepath, tmp_file.name)





#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#airflow/airflow/providers/apache/hive/transfers/hive_to_samba.py 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
This module contains operator to move data from Hive to Samba.
"""

from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars


class HiveToSambaOperator(BaseOperator):
    """
    Executes hql code in a specific Hive database and loads the
    results of the query as a csv to a Samba location.
    :param hql: the hql to be exported. (templated)
    :type hql: str
    :param destination_filepath: the file path to where the file will be pushed onto samba
    :type destination_filepath: str
    :param samba_conn_id: reference to the samba destination
    :type samba_conn_id: str
    :param hiveserver2_conn_id: reference to the hiveserver2 service
    :type hiveserver2_conn_id: str
    """

    template_fields = ('hql', 'destination_filepath')
    template_ext = (
        '.hql',
        '.sql',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        hql: str,
        destination_filepath: str,
        samba_conn_id: str = 'samba_default',
        hiveserver2_conn_id: str = 'hiveserver2_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.samba_conn_id = samba_conn_id
        self.destination_filepath = destination_filepath
        self.hql = hql.strip().rstrip(';')

    def execute(self, context):
        with NamedTemporaryFile() as tmp_file:
            self.log.info("Fetching file from Hive")
            hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)
            hive.to_csv(hql=self.hql, csv_filepath=tmp_file.name, hive_conf=context_to_airflow_vars(context))
            self.log.info("Pushing to samba")
            samba = SambaHook(samba_conn_id=self.samba_conn_id)
            samba.push_from_local(self.destination_filepath, tmp_file.name)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#AirflowWorkflow/tests/providers/apache/hive/operators/test_hive_to_samba.py 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import os
import unittest
from unittest.mock import Mock, PropertyMock, patch

from airflow.providers.apache.hive.operators.hive_to_samba import Hive2SambaOperator
from airflow.utils.operator_helpers import context_to_airflow_vars
from tests.providers.apache.hive import DEFAULT_DATE, TestHiveEnvironment


class TestHive2SambaOperator(TestHiveEnvironment):

    def setUp(self):
        self.kwargs = dict(
            hql='hql',
            destination_filepath='destination_filepath',
            samba_conn_id='samba_default',
            hiveserver2_conn_id='hiveserver2_default',
            task_id='test_hive_to_samba_operator',
        )
        super().setUp()

    @patch('airflow.providers.apache.hive.operators.hive_to_samba.SambaHook')
    @patch('airflow.providers.apache.hive.operators.hive_to_samba.HiveServer2Hook')
    @patch('airflow.providers.apache.hive.operators.hive_to_samba.NamedTemporaryFile')
    def test_execute(self, mock_tmp_file, mock_hive_hook, mock_samba_hook):
        type(mock_tmp_file).name = PropertyMock(return_value='tmp_file')
        mock_tmp_file.return_value.__enter__ = Mock(return_value=mock_tmp_file)
        context = {}

        Hive2SambaOperator(**self.kwargs).execute(context)

        mock_hive_hook.assert_called_once_with(hiveserver2_conn_id=self.kwargs['hiveserver2_conn_id'])
        mock_hive_hook.return_value.to_csv.assert_called_once_with(
            hql=self.kwargs['hql'],
            csv_filepath=mock_tmp_file.name,
            hive_conf=context_to_airflow_vars(context))
        mock_samba_hook.assert_called_once_with(samba_conn_id=self.kwargs['samba_conn_id'])
        mock_samba_hook.return_value.push_from_local.assert_called_once_with(
            self.kwargs['destination_filepath'], mock_tmp_file.name)

    @unittest.skipIf(
        'AIRFLOW_RUNALL_TESTS' not in os.environ,
        "Skipped because AIRFLOW_RUNALL_TESTS is not set")
    def test_hive2samba(self):
        op = Hive2SambaOperator(
            task_id='hive2samba_check',
            samba_conn_id='tableau_samba',
            hql="SELECT * FROM airflow.static_babynames LIMIT 10000",
            destination_filepath='test_airflow.csv',
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#air-flow/airflow/hooks/samba_hook.py 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""This module is deprecated. Please use `airflow.providers.samba.hooks.samba`."""

import warnings

# pylint: disable=unused-import
from airflow.providers.samba.hooks.samba import SambaHook  # noqa

warnings.warn(
    "This module is deprecated. Please use `airflow.providers.samba.hooks.samba`.",
    DeprecationWarning, stacklevel=2
)

