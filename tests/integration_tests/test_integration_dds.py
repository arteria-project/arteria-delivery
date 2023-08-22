import json
import pathlib
import time
import tempfile

from tornado.testing import *
from tornado import gen

from delivery.models.db_models import StagingStatus, DeliveryStatus

from tests.integration_tests.base import BaseIntegration


class TestIntegrationDDS(BaseIntegration):
    @gen_test
    def test_can_stage_and_delivery_runfolder(self):
        # Note that this is a test which skips delivery (since to_outbox is not
        # expected to be installed on the system where this runs)

        with tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)
            self._create_checksums_file(tmp_dir)

            project_name = "AB-1234"
            url = "/".join([self.API_BASE, "dds_project", "create", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            dds_project_id = json.loads(response.body)["dds_project_id"]

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
                self.assertTrue(os.path.exists(f"/tmp/{staging_id}/{project}"))
                delivery_url = '/'.join([self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {
                        'delivery_project_id': dds_project_id,
                        'ngi_project_name': project_name,
                        'auth_token': '1234',
                        'skip_delivery': True,
                        }
                delivery_resp = yield self.http_client.fetch(
                        self.get_url(delivery_url),
                        method='POST',
                        body=json.dumps(delivery_body))
                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                status_response = yield self.http_client.fetch(delivery_link)
                self.assertEqual(json.loads(status_response.body)["status"], DeliveryStatus.delivery_skipped.name)

    @gen_test
    def test_can_stage_and_delivery_project_dir(self):
        # Note that this is a test which skips delivery (since to_outbox is not
        # expected to be installed on the system where this runs)

        with tempfile.TemporaryDirectory(dir='./tests/resources/projects') as tmp_dir:
            project_name = "AB-1234"
            url = "/".join([self.API_BASE, "dds_project", "create", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            dds_project_id = json.loads(response.body)["dds_project_id"]

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
                        'delivery_project_id': dds_project_id,
                        'ngi_project_name': project_name,
                        'skip_delivery': True,
                        'dds': True,
                        'auth_token': '1234',
                        }
                delivery_resp = yield self.http_client.fetch(self.get_url(delivery_url), method='POST', body=json.dumps(delivery_body))
                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                status_response = yield self.http_client.fetch(delivery_link)
                self.assertEqual(json.loads(status_response.body)["status"], DeliveryStatus.delivery_skipped.name)

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

    @gen_test
    def test_can_create_project(self):
        project_name = "CD-1234"
        url = "/".join([self.API_BASE, "dds_project", "create", project_name])
        payload = {
            "description": "Dummy project",
            "pi": "alex@doe.com",
            "researchers": ["robin@doe.com", "kim@doe.com"],
            "owners": ["alex@doe.com"],
            "auth_token": '1234',
        }

        response = yield self.http_client.fetch(
                self.get_url(url), method='POST',
                body=json.dumps(payload))

        self.assertEqual(response.code, 202)
        self.assertTrue(json.loads(response.body)["dds_project_id"].startswith("snpseq"))

    @gen_test
    def test_can_create_two_projects(self):
        project_name = "CD-1234"
        url = "/".join([self.API_BASE, "dds_project", "create", project_name])
        payload = {
            "description": "Dummy project",
            "pi": "alex@doe.com",
            "researchers": ["robin@doe.com", "kim@doe.com"],
            "owners": ["alex@doe.com"],
            "auth_token": '1234',
        }

        response = yield self.http_client.fetch(
                self.get_url(url), method='POST',
                body=json.dumps(payload))
        self.assertEqual(response.code, 202)
        dds_project_id1 = json.loads(response.body)["dds_project_id"]

        response = yield self.http_client.fetch(
                self.get_url(url), method='POST',
                body=json.dumps(payload))
        self.assertEqual(response.code, 202)
        dds_project_id2 = json.loads(response.body)["dds_project_id"]

        self.assertNotEqual(dds_project_id1, dds_project_id2)

    @gen_test
    def test_can_deliver_project_dir_stage_free(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/projects') as tmp_dir:
            project_name = "AB-1235"
            url = "/".join([self.API_BASE, "deliver", "project", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
                "project_alias": pathlib.Path(tmp_dir).name,
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            response_body = json.loads(response.body)
            status_link = response_body["status_link"]

            while True:
                status_response = yield self.http_client.fetch(
                        status_link)
                self.assertEqual(status_response.code, 200)
                if (json.loads(status_response.body)["status"]
                        == DeliveryStatus.delivery_successful.name):
                    break
                elif (json.loads(status_response.body)["status"]
                        == DeliveryStatus.delivery_failed.name):
                    raise Exception("Delivery failed")
                time.sleep(1)

    @gen_test
    def test_can_deliver_project_dir_stage_free_no_alias(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/projects') as tmp_dir:
            project_name = pathlib.Path(tmp_dir).name
            url = "/".join([self.API_BASE, "deliver", "project", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            response_body = json.loads(response.body)
            status_link = response_body["status_link"]

            while True:
                status_response = yield self.http_client.fetch(
                        status_link)
                self.assertEqual(status_response.code, 200)
                if (json.loads(status_response.body)["status"]
                        == DeliveryStatus.delivery_successful.name):
                    break
                elif (json.loads(status_response.body)["status"]
                        == DeliveryStatus.delivery_failed.name):
                    raise Exception("Delivery failed")
                time.sleep(1)

    @gen_test(timeout=20)
    def test_double_project_delivery_protection(self):
        @gen.coroutine
        def deliver_project_old_route(project_name, project_path, force=False):

            dir_name = project_path.name

            payload = {"force_delivery": force}
            if project_name:
                url = "/".join(
                        [self.API_BASE, "stage", "project", project_name])
                payload["project_alias"] = project_path.name
            else:
                url = "/".join([self.API_BASE, "stage", "project", dir_name])

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload),
                    raise_error=False)

            # Wait for staging to complete before returning
            if response.code == 202:
                response_json = json.loads(response.body)
                staging_status_links = response_json.get("staging_order_links")

                for project, link in staging_status_links.items():
                    status_response = yield self.http_client.fetch(link)
                    self.assertEqual(
                        json.loads(status_response.body)["status"],
                        StagingStatus.staging_successful.name
                    )

            return response

        @gen.coroutine
        def deliver_project_new_route(project_name, project_path, force=False):
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
                "force_delivery": force,
            }

            if project_name:
                url = "/".join(
                    [self.API_BASE, "deliver", "project", project_name])
                payload["project_alias"] = project_path.name
            else:
                url = "/".join(
                    [self.API_BASE, "deliver", "project", project_path.name])

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload),
                    raise_error=False,
                    )

            # Wait for staging to complete before returning
            if response.code == 202:
                response_body = json.loads(response.body)
                status_link = response_body["status_link"]

                while True:
                    status_response = yield self.http_client.fetch(
                            status_link)
                    self.assertEqual(status_response.code, 200)
                    if (json.loads(status_response.body)["status"]
                            == DeliveryStatus.delivery_successful.name):
                        break
                    elif (json.loads(status_response.body)["status"]
                            == DeliveryStatus.delivery_failed.name):
                        raise Exception("Delivery failed")
                    time.sleep(1)

            return response

        delivery_methods = [
            deliver_project_old_route,
            deliver_project_new_route,
        ]

        for (i, (first_delivery, second_delivery, project_name)) in enumerate([
            (first_delivery, second_delivery, project_name)
            for first_delivery in delivery_methods
            for second_delivery in delivery_methods
            for project_name in ["AB-{:04d}", None]
        ]):
            with tempfile.TemporaryDirectory(
                    dir='./tests/resources/projects') as tmp_dir:
                if project_name:
                    project_name = project_name.format(i)
                project_path = pathlib.Path(tmp_dir)

                response = yield first_delivery(
                        project_name, project_path)
                self.assertEqual(response.code, 202)

                response = yield second_delivery(
                        project_name, project_path)
                self.assertEqual(response.code, 403)

                response = yield second_delivery(
                        project_name, project_path, force=True)
                self.assertEqual(response.code, 202)

    @gen_test
    def test_getting_unknown_status_returns_not_found(self):
        url = "/".join([self.API_BASE, "deliver", "status", "snpseq00000"])
        response = yield self.http_client.fetch(
            self.get_url(url),
            raise_error=False,
        )
        self.assertEqual(response.code, 404)

class TestIntegrationDDSShortWait(BaseIntegration):
    def __init__(self, *args):
        super().__init__(*args)

        self.mock_duration = 2

    @gen_test(timeout=5)
    def test_mock_duration_is_2(self):
        with tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)
            self._create_checksums_file(tmp_dir)

            project_name = "AB-1230"
            url = "/".join([self.API_BASE, "dds_project", "create", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            dds_project_id = json.loads(response.body)["dds_project_id"]

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = yield self.http_client.fetch(
                    self.get_url(url),
                    method='POST',
                    body='')

            response_json = json.loads(response.body)

            staging_order_project_and_id = response_json.get("staging_order_ids")

            for project, staging_id in staging_order_project_and_id.items():
                delivery_url = '/'.join([
                    self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {
                        'delivery_project_id': dds_project_id,
                        'ngi_project_name': project_name,
                        'dds': True,
                        'auth_token': '1234',
                        'skip_delivery': False,
                        }

                start = time.time()

                delivery_resp = yield self.http_client.fetch(
                        self.get_url(delivery_url),
                        method='POST',
                        body=json.dumps(delivery_body))

                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                while True:
                    status_response = yield self.http_client.fetch(
                            delivery_link)
                    if (json.loads(status_response.body)["status"]
                            == DeliveryStatus.delivery_successful.name):
                        break

                stop = time.time()
                self.assertTrue(stop - start >= self.mock_duration)

    @gen_test(timeout=5)
    def test_can_delivery_data_asynchronously(self):
        with tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)
            self._create_checksums_file(tmp_dir)

            project_name = "AB-1233"
            url = "/".join([self.API_BASE, "dds_project", "create", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            dds_project_id = json.loads(response.body)["dds_project_id"]

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = yield self.http_client.fetch(
                    self.get_url(url),
                    method='POST',
                    body='')

            response_json = json.loads(response.body)

            staging_order_project_and_id = response_json.get("staging_order_ids")

            for project, staging_id in staging_order_project_and_id.items():
                delivery_url = '/'.join([
                    self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {
                        'delivery_project_id': dds_project_id,
                        'ngi_project_name': project_name,
                        'dds': True,
                        'auth_token': '1234',
                        'skip_delivery': False,
                        }

                delivery_resp = yield self.http_client.fetch(
                        self.get_url(delivery_url),
                        method='POST',
                        body=json.dumps(delivery_body))

                delivery_resp_as_json = json.loads(delivery_resp.body)
                delivery_link = delivery_resp_as_json['delivery_order_link']

                while True:
                    status_response = yield self.http_client.fetch(
                            delivery_link)
                    if (json.loads(status_response.body)["status"]
                            == DeliveryStatus.delivery_successful.name):
                        break
                    elif (json.loads(status_response.body)["status"]
                            == DeliveryStatus.delivery_failed.name):
                        raise Exception("Delivery failed")

    @gen_test
    def test_can_deliver_project_dir_stage_free_short_wait(self):
        with tempfile.TemporaryDirectory(dir='./tests/resources/projects') as tmp_dir:
            project_name = "AB-1235"
            url = "/".join([self.API_BASE, "deliver", "project", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
                "project_alias": pathlib.Path(tmp_dir).name,
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            response_body = json.loads(response.body)
            status_link = response_body["status_link"]

            while True:
                status_response = yield self.http_client.fetch(
                        status_link)
                self.assertEqual(status_response.code, 200)
                if (json.loads(status_response.body)["status"]
                        == DeliveryStatus.delivery_successful.name):
                    break
                elif (json.loads(status_response.body)["status"]
                        == DeliveryStatus.delivery_failed.name):
                    raise Exception("Delivery failed")
                time.sleep(1)


class TestIntegrationDDSLongWait(BaseIntegration):
    def __init__(self, *args):
        super().__init__(*args)

        self.mock_duration = 10

    @gen_test
    def test_can_deliver_and_not_timeout(self):
        """
        This test checks that the service does not wait for the full duration
        of the delivery (10s in this case) to respond. If it does wait, it will
        raise a time-out error after 5s (default duration of tornado tests).
        """
        with tempfile.TemporaryDirectory(
                dir='./tests/resources/runfolders/',
                prefix='160930_ST-E00216_0111_BH37CWALXX_') as tmp_dir:

            project_name = "CD-1234"
            url = "/".join([self.API_BASE, "dds_project", "create", project_name])
            payload = {
                "description": "Dummy project",
                "pi": "alex@doe.com",
                "researchers": ["robin@doe.com", "kim@doe.com"],
                "owners": ["alex@doe.com"],
                "auth_token": '1234',
            }

            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST',
                    body=json.dumps(payload))

            self.assertEqual(response.code, 202)
            dds_project_id = json.loads(response.body)["dds_project_id"]

            dir_name = os.path.basename(tmp_dir)
            self._create_projects_dir_with_random_data(tmp_dir)
            self._create_checksums_file(tmp_dir)

            url = "/".join([self.API_BASE, "stage", "runfolder", dir_name])
            response = yield self.http_client.fetch(
                    self.get_url(url), method='POST', body='')

            response_json = json.loads(response.body)

            staging_order_project_and_id = response_json.get(
                    "staging_order_ids")

            for project, staging_id in staging_order_project_and_id.items():
                delivery_url = '/'.join([
                    self.API_BASE, 'deliver', 'stage_id', str(staging_id)])
                delivery_body = {
                        'delivery_project_id': dds_project_id,
                        'ngi_project_name': project_name,
                        'dds': True,
                        'auth_token': '1234',
                        'skip_delivery': False,
                        }
                delivery_response = yield self.http_client.fetch(
                        self.get_url(delivery_url),
                        method='POST',
                        body=json.dumps(delivery_body))
                self.assertEqual(delivery_response.code, 202)


class TestIntegrationDDSUnmocked(BaseIntegration):
    def __init__(self, *args):
        super().__init__(*args)

        self.mock_delivery = False

    @gen_test
    def test_responds_with_dummy_token(self):
        project_name = "CD-1234"
        url = "/".join([self.API_BASE, "dds_project", "create", project_name])
        payload = {
            "description": "Dummy project",
            "pi": "alex@doe.com",
            "researchers": ["robin@doe.com", "kim@doe.com"],
            "owners": ["alex@doe.com"],
            "auth_token": '1234',
        }

        response = yield self.http_client.fetch(
                self.get_url(url), method='POST',
                body=json.dumps(payload),
                raise_error=False,
                )

        self.assertEqual(response.code, 500)
