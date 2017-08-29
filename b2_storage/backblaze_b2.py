import os
import base64
import hashlib

import requests


AUTH_URL = 'https://api.backblaze.com/b2api/v1/b2_authorize_account'


class BackBlazeB2(object):

    authorization_token = None

    def __init__(self, app_key=None, account_id=None, bucket_name=None,
                 reupload_attempts=3):
        self.bucket_id = None
        self.account_id = account_id
        self.app_key = app_key
        self.bucket_name = bucket_name
        self.reupload_attempts = reupload_attempts

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

        if response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def _build_url(self, endpoint):
        return self.base_url + endpoint

    def upload_file(self, name, content):
        self._ensure_authorization()

        response = self.get_upload_url()

        url = response['uploadUrl']
        sha1_of_file_data = hashlib.sha1(content.read()).hexdigest()
        content.seek(0)

        headers = {
            'Authorization': response['authorizationToken'],
            'X-Bz-File-Name': name,
            'Content-Type': "b2/x-auto",
            'X-Bz-Content-Sha1': sha1_of_file_data,
            'X-Bz-Info-src_last_modified_millis': '',
        }

        download_response = requests.post(
            url, headers=headers, data=content.read())
        # Status is 503: Service unavailable. Try again
        if download_response.status_code == 503:
            attempts = 0
            while attempts <= self.reupload_attempts \
                    and download_response.status_code == 503:
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
        resp = requests.get(self._build_url("/b2api/v1/b2_list_buckets"),
                            headers=headers, params=params).json()
        if 'buckets' in resp:
            buckets = resp['buckets']
            for bucket in buckets:
                if bucket['bucketName'] == self.bucket_name:
                    self.bucket_id = bucket['bucketId']
                    return True

        else:
            return False
