import os
import base64
import hashlib

import logging
import requests

log = logging.getLogger('django')
AUTH_URL = 'https://api.backblazeb2.com/b2api/v2/b2_authorize_account'


class BackBlazeB2(object):
    authorization_token = None

    def __init__(self, app_key=None, account_id=None, bucket_name=None,
                 max_retries=3, content_type=None, minimum_part_size=None):
        self.bucket_id = None
        self.account_id = account_id
        self.app_key = app_key
        self.bucket_name = bucket_name
        self.content_type = content_type
        self.max_retries = max_retries
        self.minimum_part_size = minimum_part_size

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

        url = self._build_url('/b2api/v2/b2_get_upload_url')
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

    def get_upload_part_url(self, file_id):
        self._ensure_authorization()

        url = self._build_url('/b2api/v2/b2_get_upload_part_url')
        headers = {'Authorization': self.authorization_token}
        params = {'fileId': file_id}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 401:
            self.authorize()
            headers = {'Authorization': self.authorization_token}
            response = requests.get(url, headers=headers, params=params)
        elif response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def start_large_file(self, file_name):
        self._ensure_authorization()

        url = self._build_url('/b2api/v2/b2_start_large_file')
        headers = {'Authorization': self.authorization_token}
        params = {
            'fileName': file_name,
            'contentType': self.content_type or 'b2/x-auto',
            'bucketId': self.bucket_id
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 401:
            self.authorize()
            headers = {'Authorization': self.authorization_token}
            response = requests.get(url, headers=headers, params=params)
        elif response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def finish_large_file(self, file_id, part_sha1_array):
        self._ensure_authorization()

        url = self._build_url('/b2api/v2/b2_finish_large_file')
        headers = {'Authorization': self.authorization_token}
        params = {'fileId': file_id, 'partSha1Array': part_sha1_array}

        response = requests.post(url, json=params, headers=headers)

        if response.status_code == 401:
            self.authorize()
            headers = {'Authorization': self.authorization_token}
            response = requests.post(url, json=params, headers=headers)
        elif response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def _build_url(self, endpoint):
        return self.base_url + endpoint

    def upload_file(self, name, content):
        self._ensure_authorization()

        total_file_size = os.fstat(content.fileno()).st_size
        if total_file_size > self.minimum_part_size:
            return self.upload_large_file(name, content, total_file_size)

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
                    url, headers=headers, data=content.read(),
                    timeout=3600)
            except requests.exceptions.ConnectionError as e:
                log.info('Connection error: {}, current attempts: {}'.format(
                    e, attempts))
                continue

            if response.status_code == 200:
                break

            if response.status_code == 400:
                break

        if response.status_code != 200:
            response.raise_for_status()

        return response.json()

    def upload_large_file(self, name, content, total_file_size):
        start_large_file_response = self.start_large_file(name)
        file_id = start_large_file_response['fileId']
        upload_part_url_response = self.get_upload_part_url(file_id)
        url = upload_part_url_response['uploadUrl']

        size_of_part = self.minimum_part_size
        total_bytes_sent = 0
        part_number = 1
        part_sha1_array = []

        while (total_bytes_sent < total_file_size):
            if ((total_file_size - total_bytes_sent) < self.minimum_part_size):
                size_of_part = total_file_size - total_bytes_sent

            content.seek(total_bytes_sent)
            part_data = content.read(size_of_part)
            sha1_digester = hashlib.new('SHA1')
            sha1_digester.update(part_data)
            sha1_str = sha1_digester.hexdigest()
            part_sha1_array.append(sha1_str)

            headers = {
                'Authorization': upload_part_url_response['authorizationToken'],
                'X-Bz-File-Name': name,
                'X-Bz-Part-Number': str(part_number),
                'X-Bz-Content-Sha1': sha1_str,
            }

            attempts = 0
            while attempts <= self.max_retries:
                attempts += 1
                try:
                    response = requests.post(
                        url, headers=headers, data=part_data,
                        timeout=3600)
                except requests.exceptions.ConnectionError as e:
                    log.info('Connection error: {}, ' 'current attempts: {}'
                             .format(e, attempts))
                    continue

                if response.status_code == 200:
                    break
                if response.status_code == 400:
                    break

            if response.status_code != 200:
                response.raise_for_status()

            total_bytes_sent = total_bytes_sent + size_of_part
            part_number += 1

        finish_large_file_resp = self.finish_large_file(file_id,
                                                        part_sha1_array)
        assert finish_large_file_resp['contentLength'] == total_file_size

        return finish_large_file_resp

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
