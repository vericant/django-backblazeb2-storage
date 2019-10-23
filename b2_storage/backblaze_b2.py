import os
import base64
import hashlib

import requests


AUTH_URL = 'https://api.backblaze.com/b2api/v1/b2_authorize_account'


class BackBlazeB2(object):

    authorization_token = None

    def __init__(self, app_key=None, account_id=None, bucket_name=None,
                 content_type=None, max_retries=3):
        self.bucket_id = None
        self.account_id = account_id
        self.app_key = app_key
        self.bucket_name = bucket_name
        self.content_type = content_type
        self.max_retries = max_retries

    def _ensure_authorization(self):
        if self.authorization_token:
            return

        self.authorize()
        self.get_bucket_id_by_name()

    def authorize(self):
        headers = {'Authorization': 'Basic: %s' % (
            base64.b64encode(('%s:%s' % (self.account_id, self.app_key)
                              ).encode('utf-8'))).decode('utf-8')}
        response = requests.get(AUTH_URL, headers=headers)

        if response.status_code != 200:
            response.raise_for_status()

        data = response.json()

        self.base_url = data['apiUrl']
        self.download_url = data['downloadUrl']
        self.authorization_token = data['authorizationToken']

        return data

    def get_upload_url(self):
        self._ensure_authorization()

        url = self._build_url('/b2api/v1/b2_get_upload_url')
        headers = {'Authorization': self.authorization_token}
        params = {'bucketId': self.bucket_id}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 401:
            self.authorize()
            headers = {'Authorization': self.authorization_token}
            response = requests.get(url, headers=headers, params=params)
        elif response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def _build_url(self, endpoint):
        return self.base_url + endpoint

    def upload_file(self, name, content):
        self._ensure_authorization()

        upload_url_response = self.get_upload_url()

        url = upload_url_response['uploadUrl']
        sha1_of_file_data = hashlib.sha1(content.read()).hexdigest()
        content.seek(0)

        header_content_type = 'b2/x-auto'
        if self.content_type:
            header_content_type = self.content_type
        headers = {
            'Authorization': upload_url_response['authorizationToken'],
            'X-Bz-File-Name': name,
            'Content-Type': header_content_type,
            'X-Bz-Content-Sha1': sha1_of_file_data,
            'X-Bz-Info-src_last_modified_millis': '',
        }

        attempts = 0
        while attempts <= self.max_retries:
            attempts += 1
            try:
                response = requests.post(
                    url, headers=headers, data=content.read())
            except ConnectionError:
                continue

            if response.status_code == 200:
                break

        if response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def get_file_info(self, name):
        self._ensure_authorization()
        headers = {'Authorization': self.authorization_token}
        return requests.get(self.get_file_url(name), headers=headers)

    def download_file(self, name):
        return self.get_file_info(name).content

    def get_file_url(self, name):
        self._ensure_authorization()
        return os.path.join(self.download_url, 'file', self.bucket_name, name)

    def get_bucket_id_by_name(self):
        """
        BackBlaze B2 should make an endpoint to retrieve buckets by its name.
        """
        self._ensure_authorization()
        headers = {'Authorization': self.authorization_token}
        params = {'accountId': self.account_id}
        response = requests.get(self._build_url("/b2api/v1/b2_list_buckets"),
                                headers=headers, params=params)
        if response.status_code != 200:
            response.raise_for_status()

        for bucket in response.json()['buckets']:
            if bucket['bucketName'] == self.bucket_name:
                self.bucket_id = bucket['bucketId']
