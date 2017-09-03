import os
import base64
import hashlib

import requests


AUTH_URL = 'https://api.backblaze.com/b2api/v1/b2_authorize_account'


class BackBlazeB2(object):

    authorization_token = None

    def __init__(self, app_key=None, account_id=None, bucket_name=None,
                 max_retries=3):
        self.bucket_id = None
        self.account_id = account_id
        self.app_key = app_key
        self.bucket_name = bucket_name
        self.max_retries = max_retries

    def _ensure_authorization(self, force=False):
        if self.authorization_token and not force:
            return

        self.authorize()
        self.get_bucket_id_by_name()

    def authorize(self):
        headers = {'Authorization': 'Basic: %s' % (
            base64.b64encode(('%s:%s' % (self.account_id, self.app_key)
                              ).encode('utf-8'))).decode('utf-8')}
        response = requests.get(AUTH_URL, headers=headers)

        if response.status_code == 200:
            resp = response.json()
            self.base_url = resp['apiUrl']
            self.download_url = resp['downloadUrl']
            self.authorization_token = resp['authorizationToken']
            return True
        else:
            return False

    def get_upload_url(self):
        self._ensure_authorization()

        url = self._build_url('/b2api/v1/b2_get_upload_url')
        headers = {'Authorization': self.authorization_token}
        params = {'bucketId': self.bucket_id}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 401:
            self._ensure_authorization(force=True)
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

        headers = {
            'Authorization': upload_url_response['authorizationToken'],
            'X-Bz-File-Name': name,
            'Content-Type': "b2/x-auto",
            'X-Bz-Content-Sha1': sha1_of_file_data,
            'X-Bz-Info-src_last_modified_millis': '',
        }

        response = requests.post(url, headers=headers, data=content.read())

        if response.status_code != 200:
            attempts = 0
            while attempts <= self.max_retries and response.status_code == 503:
                download_response = requests.post(
                    url, headers=headers, data=content.read())
                attempts += 1
        if download_response.status_code != 200:
            download_response.raise_for_status()

        return download_response.json()

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
