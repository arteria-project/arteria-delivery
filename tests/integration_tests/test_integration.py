

import json
from functools import partial
import sys
import tempfile

from tornado.testing import *
from tornado.web import Application

from arteria.web.app import AppService

from delivery.app import routes as app_routes, compose_application
from delivery.models.db_models import StagingStatus, DeliveryStatus

from tests.test_utils import assert_eventually_equals


class TestPythonVersion(unittest.TestCase):
    """
    Ensure the python binary is of a compatible version
    """
    REQUIRED_MAJOR_VERSION = 3
    REQUIRED_MINOR_VERSION = 6

    def test_python_binary_version(self):
        self.assertEqual(TestPythonVersion.REQUIRED_MAJOR_VERSION, sys.version_info.major)
        self.assertLessEqual(TestPythonVersion.REQUIRED_MINOR_VERSION, sys.version_info.minor)


class TestIntegration(AsyncHTTPTestCase):

    def _get_delivery_status(self, link):
        self.http_client.fetch(link, self.stop)
        status_response = self.wait()
        return json.loads(status_response.body)["status"]

    def _get_size(self, staging_link):
        self.http_client.fetch(staging_link, self.stop)
        status_response = self.wait()
        return json.loads(status_response.body)["size"]

    def _create_projects_dir_with_random_data(self, base_dir, proj_name='ABC_123'):
        tmp_proj_dir = os.path.join(base_dir, 'Projects', proj_name)
        os.makedirs(tmp_proj_dir)
        with open(os.path.join(tmp_proj_dir, 'test_file'), 'wb') as f:
            f.write(os.urandom(1024))

    API_BASE = "/api/1.0"

    def get_app(self):

        # Get an as similar app as possible, tough note that we don't use the
        #  app service start method to start up the the application
        path_to_this_file = os.path.abspath(
            os.path.dirname(os.path.realpath(__file__)))
        app_svc = AppService.create(product_name="test_delivery_service",
                                    config_root="{}/../../config/".format(path_to_this_file))

        config = app_svc.config_svc

        composed_application = compose_application(config)
        # TODO Later swap the "real" delivery service here for mock one.

        return Application(app_routes(**composed_application))

    def test_can_return_flowcells(self):
        response = self.fetch(self.API_BASE + "/runfolders")

        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        runfolder_names = []
        for runfolder_json in response_json["runfolders"]:
            runfolder_names.append(runfolder_json["name"])

        self.assertIn("160930_ST-E00216_0112_AH37CWALXX", runfolder_names)

        self.assertIn("160930_ST-E00216_0111_BH37CWALXX", runfolder_names)

    def test_can_return_projects(self):
        response = self.fetch(self.API_BASE + "/projects")
        self.assertEqual(response.code, 200)

        response_json = json.loads(response.body)
        self.assertEqual(len(response_json), 1)

        first_project = response_json["projects"][0]
        self.assertEqual(first_project["name"], "ABC_123")

    def test_can_stage_and_delivery_runfolder(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/', prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 202)

            response_json = json.loads(response.body)

            staging_status_links = response_json.get("staging_order_links")

            for project, link in staging_status_links.items():

                self.assertEqual(project, "ABC_123")

                assert_eventually_equals(self,
                                         timeout=5,
                                         delay=1,
                                         f=partial(self._get_delivery_status, link),
                                         expected=StagingStatus.staging_successful.name)

                # The size of the fake project is 1024 bytes
                assert_eventually_equals(self,
                                         timeout=5,
                                         delay=1,
                                         f=partial(self._get_size, link),
                                         expected=1024)

            staging_order_project_and_id = response_json.get("staging_order_ids")

            for project, staging_id in staging_order_project_and_id.items():
                delivery_url = '/'.join([self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {'delivery_project_id': 'fakedeliveryid2016',
                                 'skip_mover': True}
                delivery_resp = self.fetch(delivery_url, method='POST', body=json.dumps(delivery_body))
                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                assert_eventually_equals(self,
                                         timeout=5,
                                         delay=1,
                                         f=partial(self._get_delivery_status, delivery_link),
                                         expected=DeliveryStatus.delivery_skipped.name)

    def test_cannot_stage_the_same_runfolder_twice(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/', prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 202)

            response = self.fetch(url, method='POST', body='')
            print(response.reason)
            self.assertEqual(response.code, 403)

            # Unless you force the delivery
            response = self.fetch(url, method='POST', body=json.dumps({"force_delivery": True}))
            self.assertEqual(response.code, 202)

    def test_can_stage_and_delivery_project_dir(self):
        # Note that this is a test which skips mover (since to_outbox is not expected to be installed on the system
        # where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/projects') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            url = "/".join([self.API_BASE, "stage", "project", dir_name])
            response = self.fetch(url, method='POST', body='')
            self.assertEqual(response.code, 202)

            response_json = json.loads(response.body)

            staging_status_links = response_json.get("staging_order_links")

            for project, link in staging_status_links.items():
                self.assertEqual(project, dir_name)

                assert_eventually_equals(self,
                                         timeout=5,
                                         delay=1,
                                         f=partial(self._get_delivery_status, link),
                                         expected=StagingStatus.staging_successful.name)

            staging_order_project_and_id = response_json.get("staging_order_ids")

            for project, staging_id in staging_order_project_and_id.items():
                delivery_url = '/'.join([self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {'delivery_project_id': 'fakedeliveryid2016',
                                 'skip_mover': True}
                delivery_resp = self.fetch(delivery_url, method='POST', body=json.dumps(delivery_body))
                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                assert_eventually_equals(self,
                                         timeout=5,
                                         delay=1,
                                         f=partial(self._get_delivery_status, delivery_link),
                                         expected=DeliveryStatus.delivery_skipped.name)

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

    def test_can_stage_and_deliver_clean_flowcells(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix='160930_ST-E00216_0555_BH37CWALXX_') as tmpdir1,\
             tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix='160930_ST-E00216_0556_BH37CWALXX_') as tmpdir2:
                self._create_projects_dir_with_random_data(tmpdir1, 'XYZ_123')
                self._create_projects_dir_with_random_data(tmpdir2, 'XYZ_123')

                url = "/".join([self.API_BASE, "stage", "project", 'runfolders', 'XYZ_123'])
                payload = {'delivery_mode': 'CLEAN'}
                response = self.fetch(url, method='POST', body=json.dumps(payload))
                self.assertEqual(response.code, 202)

                payload = {'delivery_mode': 'CLEAN'}
                response_failed = self.fetch(url, method='POST', body=json.dumps(payload))
                self.assertEqual(response_failed.code, 403)

                response_json = json.loads(response.body)

                staging_status_links = response_json.get("staging_order_links")

                for project, link in staging_status_links.items():
                    self.assertEqual(project, 'XYZ_123')

                assert_eventually_equals(self,
                                         timeout=5,
                                         delay=1,
                                         f=partial(self._get_delivery_status, link),
                                         expected=StagingStatus.staging_successful.name)

    def test_can_stage_and_deliver_batch_flowcells(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                         prefix='160930_ST-E00216_0555_BH37CWALXX_') as tmpdir1, \
                tempfile.TemporaryDirectory(dir='./tests/resources/runfolders/',
                                            prefix='160930_ST-E00216_0556_BH37CWALXX_') as tmpdir2:
            self._create_projects_dir_with_random_data(tmpdir1, 'XYZ_123')
            self._create_projects_dir_with_random_data(tmpdir2, 'XYZ_123')

            url = "/".join([self.API_BASE, "stage", "project", 'runfolders', 'XYZ_123'])
            payload = {'delivery_mode': 'BATCH'}
            response = self.fetch(url, method='POST', body=json.dumps(payload))
            self.assertEqual(response.code, 202)

            payload = {'delivery_mode': 'BATCH'}
            response_failed = self.fetch(url, method='POST', body=json.dumps(payload))
            self.assertEqual(response_failed.code, 403)

            response_json = json.loads(response.body)

            staging_status_links = response_json.get("staging_order_links")

            for project, link in staging_status_links.items():
                self.assertEqual(project, 'XYZ_123')

            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=partial(self._get_delivery_status, link),
                                     expected=StagingStatus.staging_successful.name)

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
            response = self.fetch(url, method='POST', body=json.dumps(payload))
            self.assertEqual(response.code, 202)

            # The it should be denied (since if has already been staged)
            payload = {'delivery_mode': 'BATCH'}
            response_failed = self.fetch(url, method='POST', body=json.dumps(payload))
            self.assertEqual(response_failed.code, 403)

            # Then it should work once force is specified.
            payload = {'delivery_mode': 'FORCE'}
            response_forced = self.fetch(url, method='POST', body=json.dumps(payload))
            self.assertEqual(response_forced.code, 202)

            response_json = json.loads(response_forced.body)

            staging_status_links = response_json.get("staging_order_links")

            for project, link in staging_status_links.items():
                self.assertEqual(project, 'XYZ_123')

            assert_eventually_equals(self,
                                     timeout=5,
                                     delay=1,
                                     f=partial(self._get_delivery_status, link),
                                     expected=StagingStatus.staging_successful.name)

