

import json
from functools import partial
import sys
import time
import tempfile

from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.models.db_models import StagingStatus, DeliveryStatus
from delivery.services.metadata_service import MetadataService

from tests.integration_tests.base import BaseIntegration
from tests.test_utils import assert_eventually_equals, unorganised_runfolder, samplesheet_file_from_runfolder, \
    project_report_files

class TestIntegrationMover(BaseIntegration):
    @gen_test
    def test_can_stage_and_delivery_runfolder(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/', prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)
            self._create_checksums_file(tmp_dir)

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = yield self.http_client.fetch(self.get_url(url), method='POST', body='')
            self.assertEqual(response.code, 202)

            response_json = json.loads(response.body)

            staging_status_links = response_json.get("staging_order_links")

            for project, link in staging_status_links.items():

                self.assertEqual(project, "ABC_123")

                status_response = yield self.http_client.fetch(link)
                self.assertEqual(json.loads(status_response.body)["status"], StagingStatus.staging_successful.name)


                # The size of the fake project is 1024 bytes
                status_response = yield self.http_client.fetch(link)
                self.assertEqual(json.loads(status_response.body)["size"], 1024)

            staging_order_project_and_id = response_json.get("staging_order_ids")

            for project, staging_id in staging_order_project_and_id.items():
                self.assertTrue(os.path.exists(f"/tmp/{staging_id}"))
                delivery_url = '/'.join([self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {
                        'delivery_project_id': 'fakedeliveryid2016',
                        'dds': True,
                        'token_path': 'token_path',
                        'skip_mover': True
                        }
                delivery_resp = yield self.http_client.fetch(self.get_url(delivery_url), method='POST', body=json.dumps(delivery_body))
                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                status_response = yield self.http_client.fetch(delivery_link)
                self.assertEqual(json.loads(status_response.body)["status"], DeliveryStatus.delivery_skipped.name)

                self.assertTrue(not os.path.exists(f"/tmp/{staging_id}/{project}"))

    def test_cannot_stage_the_same_runfolder_twice(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/', prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)
            self._create_checksums_file(tmp_dir)

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 202)

            response = self.fetch(url, method='POST', body='')
            print(response.reason)
            self.assertEqual(response.code, 403)

            # Unless you force the delivery
            response = self.fetch(url, method='POST', body=json.dumps({"force_delivery": True}))
            self.assertEqual(response.code, 202)

    @gen_test
    def test_can_stage_and_delivery_project_dir(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/projects') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            url = "/".join([self.API_BASE, "stage", "project", dir_name])
            response = yield self.http_client.fetch(self.get_url(url), method='POST', body='')
            self.assertEqual(response.code, 202)

            response_json = json.loads(response.body)

            staging_status_links = response_json.get("staging_order_links")

            for project, link in staging_status_links.items():
                self.assertEqual(project, dir_name)

                status_response = yield self.http_client.fetch(link)
                self.assertEqual(json.loads(status_response.body)["status"], StagingStatus.staging_successful.name)

            staging_order_project_and_id = response_json.get("staging_order_ids")

            for project, staging_id in staging_order_project_and_id.items():
                delivery_url = '/'.join([self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {
                        'delivery_project_id': 'fakedeliveryid2016',
                        'skip_mover': True,
                        'dds': True,
                        'token_path': 'token_path',
                        }
                delivery_resp = yield self.http_client.fetch(self.get_url(delivery_url), method='POST', body=json.dumps(delivery_body))
                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                status_response = yield self.http_client.fetch(delivery_link)
                self.assertEqual(json.loads(status_response.body)["status"], DeliveryStatus.delivery_skipped.name)

    def test_cannot_stage_the_same_project_twice(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/projects') as tmp_dir:

            # Stage once should work
            dir_name = os.path.basename(tmp_dir)
            url = "/".join([self.API_BASE, "stage", "project", dir_name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 202)

            # The second time should not
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 403)

            # Unless you force the delivery
            response = self.fetch(url, method='POST', body=json.dumps({"force_delivery": True}))
            self.assertEqual(response.code, 202)

    @gen_test
    def test_can_stage_and_deliver_clean_flowcells(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix='160930_ST-E00216_0555_BH37CWALXX_') as tmpdir1,\
             tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix='160930_ST-E00216_0556_BH37CWALXX_') as tmpdir2:
                self._create_projects_dir_with_random_data(tmpdir1, 'XYZ_123')
                self._create_projects_dir_with_random_data(tmpdir2, 'XYZ_123')

                url = "/".join([self.API_BASE, "stage", "project", 'runfolders', 'XYZ_123'])
                payload = {'delivery_mode': 'CLEAN'}
                response = yield self.http_client.fetch(self.get_url(url), method='POST', body=json.dumps(payload))
                self.assertEqual(response.code, 202)

                payload = {'delivery_mode': 'CLEAN'}
                response_failed = yield self.http_client.fetch(self.get_url(url), method='POST', body=json.dumps(payload), raise_error=False)
                self.assertEqual(response_failed.code, 403)

                response_json = json.loads(response.body)

                staging_status_links = response_json.get("staging_order_links")

                for project, link in staging_status_links.items():
                    self.assertEqual(project, 'XYZ_123')

                status_response = yield self.http_client.fetch(link)
                self.assertEqual(json.loads(status_response.body)["status"], StagingStatus.staging_successful.name)

    @gen_test
    def test_can_stage_and_deliver_batch_flowcells(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix='160930_ST-E00216_0555_BH37CWALXX_') as tmpdir1, \
                tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                            prefix='160930_ST-E00216_0556_BH37CWALXX_') as tmpdir2:
            self._create_projects_dir_with_random_data(tmpdir1, 'XYZ_123')
            self._create_projects_dir_with_random_data(tmpdir2, 'XYZ_123')

            url = "/".join([self.API_BASE, "stage", "project", 'runfolders', 'XYZ_123'])
            payload = {'delivery_mode': 'BATCH'}
            response = yield self.http_client.fetch(self.get_url(url), method='POST', body=json.dumps(payload))
            self.assertEqual(response.code, 202)

            payload = {'delivery_mode': 'BATCH'}
            response_failed = yield self.http_client.fetch(self.get_url(url), method='POST', body=json.dumps(payload), raise_error=False)
            self.assertEqual(response_failed.code, 403)

            response_json = json.loads(response.body)

            staging_status_links = response_json.get("staging_order_links")

            time.sleep(1)

            for project, link in staging_status_links.items():
                self.assertEqual(project, 'XYZ_123')

                status_response = yield self.http_client.fetch(link)
                self.assertEqual(json.loads(status_response.body)["status"], StagingStatus.staging_successful.name)

            status_response = yield self.http_client.fetch(link)
            self.assertEqual(json.loads(status_response.body)["status"], StagingStatus.staging_successful.name)

    @gen_test
    def test_can_stage_and_deliver_force_flowcells(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix='160930_ST-E00216_0555_BH37CWALXX_') as tmpdir1, \
                tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                            prefix='160930_ST-E00216_0556_BH37CWALXX_') as tmpdir2:
            self._create_projects_dir_with_random_data(tmpdir1, 'XYZ_123')
            self._create_projects_dir_with_random_data(tmpdir2, 'XYZ_123')

            # First just stage it
            url = "/".join([self.API_BASE, "stage", "project", 'runfolders', 'XYZ_123'])
            payload = {'delivery_mode': 'BATCH'}
            response = yield self.http_client.fetch(self.get_url(url), method='POST', body=json.dumps(payload))
            self.assertEqual(response.code, 202)

            # The it should be denied (since if has already been staged)
            payload = {'delivery_mode': 'BATCH'}
            response_failed = yield self.http_client.fetch(self.get_url(url), method='POST', body=json.dumps(payload), raise_error=False)
            self.assertEqual(response_failed.code, 403)

            # Then it should work once force is specified.
            payload = {'delivery_mode': 'FORCE'}
            response_forced = yield self.http_client.fetch(self.get_url(url), method='POST', body=json.dumps(payload))
            self.assertEqual(response_forced.code, 202)

            response_json = json.loads(response_forced.body)

            staging_status_links = response_json.get("staging_order_links")

            for project, link in staging_status_links.items():
                self.assertEqual(project, 'XYZ_123')

            status_response = yield self.http_client.fetch(link)
            self.assertEqual(json.loads(status_response.body)["status"], StagingStatus.staging_successful.name)
