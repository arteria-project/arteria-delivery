
import json
from mock import MagicMock


from tornado.testing import *
from tornado.web import Application

from delivery.app import routes

from tests.test_utils import DummyConfig, FAKE_RUNFOLDERS


class TestProjectHandlers(AsyncHTTPTestCase):

    API_BASE = "/api/1.0"

    mock_runfolder_repo = MagicMock()
    best_practice_service = MagicMock()
    return_projects = True

    def get_app(self):
        self.mock_runfolder_repo.get_runfolders.return_value = FAKE_RUNFOLDERS
        self.mock_runfolder_repo.get_runfolder.return_value = FAKE_RUNFOLDERS[0]
        def get_projects_from_runfolders():
            if self.return_projects:
                projs = []
                for runfolder in FAKE_RUNFOLDERS:
                    for project in runfolder.projects:
                        projs.append(project)
                return projs
            else:
                return []
        self.mock_runfolder_repo.get_projects = get_projects_from_runfolders

        return Application(
            routes(
                config=DummyConfig(),
                runfolder_repo=self.mock_runfolder_repo,
                best_practice_analysis_service=self.best_practice_service))

    def test_get_projects(self):
        response = self.fetch(self.API_BASE + "/projects")

        expected_result = []
        for runfolder in FAKE_RUNFOLDERS:
            for project in runfolder.projects:
                expected_result.append(project.__dict__)

        self.assertEqual(response.code, 200)
        result = json.loads(response.body)
        self.assertDictEqual(result, {"projects": expected_result})

    def test_get_projects_empty(self):
        self.return_projects = False

        response = self.fetch(self.API_BASE + "/projects")

        self.assertEqual(response.code, 200)
        self.assertDictEqual(json.loads(response.body), {"projects": []})

    def test_get_projects_for_runfolder(self):
        response = self.fetch(
            self.API_BASE + "/runfolders/160930_ST-E00216_0111_BH37CWALXX/projects")
        self.assertEqual(response.code, 200)
        self.assertTrue(len(json.loads(response.body)["projects"]) == 3)
