# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from airflow.exceptions import AirflowException
import httplib2
import json


class CustomBigQueryHook(BigQueryHook):

    def __init__(self, conn_id, delegate_to=None):
            """
            :param conn_id: The connection ID to use when fetching connection info.
            :type conn_id: string
            :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
            :type delegate_to: string
            """
            super().__init__(bigquery_conn_id=conn_id, delegate_to=None)

    def _get_credentials(self):
        """
        Returns the Credentials object for Google API
        """
        key_path = self._get_field('key_path', False)
        keyfile_dict = self._get_field('key_path', False)
        scopes = self._get_field('scope', False)

        #print(keyfile_dict)
        #print(scopes)

        kwargs = {}
        if self.delegate_to:
            kwargs['sub'] = self.delegate_to

        if not key_path and not keyfile_dict:
            # self.log.info('Getting connection using `gcloud auth` user, since no key file '
            #               'is defined for hook.')
            credentials = GoogleCredentials.get_application_default()
        elif key_path:
            try:
                print("LOOKING INSIDE CONNECTION PANEL")
                keyfile_dict = json.loads(keyfile_dict)

                # Depending on how the JSON was formatted, it may contain
                # escaped newlines. Convert those to actual newlines.
                keyfile_dict['private_key'] = keyfile_dict['private_key'].replace(
                    '\\n', '\n')

                credentials = ServiceAccountCredentials\
                    .from_json_keyfile_dict(keyfile_dict, scopes)
            except ValueError:
                raise AirflowException('Invalid key JSON.')
        return credentials

    def _get_access_token(self):
        """
        Returns a valid access token from Google API Credentials
        """
        return self._get_credentials().get_access_token().access_token

    def get_service(self):
        """
        Returns a BigQuery service object.
        """
        http_authorized = self._authorize()
        return build('bigquery', 'v2', http=http_authorized)

    def _authorize(self):
        """
        Returns an authorized HTTP object to be used to build a Google cloud
        service hook connection.
        """
        credentials = self._get_credentials()
        http = httplib2.Http()
        return credentials.authorize(http)
