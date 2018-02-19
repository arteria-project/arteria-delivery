import unittest
import mock

import tempfile
import os

from delivery.exceptions import ProjectAlreadyDeliveredException

from delivery.models.project import RunfolderProject, GeneralProject
from delivery.models.db_models import StagingOrder, StagingStatus, DeliverySource
from delivery.models.delivery_modes import DeliveryMode
from delivery.services.delivery_service import DeliveryService
from delivery.services.file_system_service import FileSystemService

from delivery.services.mover_service import MoverDeliveryService
from delivery.services.staging_service import StagingService
from delivery.services.runfolder_service import RunfolderService

from delivery.repositories.delivery_sources_repository import DatabaseBasedDeliverySourcesRepository
from delivery.repositories.project_repository import GeneralProjectRepository

class TestDeliveryService(unittest.TestCase):

    runfolder_projects = [RunfolderProject(name="ABC_123",
                                           path="/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/ABC_123",
                                           runfolder_path="/foo/160930_ST-E00216_0112_BH37CWALXX",
                                           runfolder_name="160930_ST-E00216_0112_BH37CWALXX"),
                          RunfolderProject(name="ABC_123",
                                           path="/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                                           runfolder_path="/foo/160930_ST-E00216_0111_BH37CWALXX/",
                                           runfolder_name="160930_ST-E00216_0111_BH37CWALXX")]

    general_project = GeneralProject(name="ABC_123", path="/foo/bar/ABC_123")

    def _compose_delivery_service(self,
                                  mover_delivery_service=mock.create_autospec(MoverDeliveryService),
                                  staging_service=mock.create_autospec(StagingService),
                                  delivery_sources_repo=mock.create_autospec(DatabaseBasedDeliverySourcesRepository),
                                  general_project_repo=mock.create_autospec(GeneralProjectRepository),
                                  runfolder_service=mock.create_autospec(RunfolderService),
                                  file_system_service=FileSystemService,
                                  project_links_dir=mock.MagicMock()):
        mover_delivery_service = mover_delivery_service
        self.staging_service = staging_service
        delivery_sources_repo = delivery_sources_repo
        general_project_repo = general_project_repo
        runfolder_service = runfolder_service
        self.file_system_service = file_system_service
        self.project_links_dir = project_links_dir

        self.delivery_service = DeliveryService(mover_service=mover_delivery_service,
                                                staging_service=self.staging_service,
                                                delivery_sources_repo=delivery_sources_repo,
                                                general_project_repo=general_project_repo,
                                                runfolder_service=runfolder_service,
                                                file_system_service=file_system_service,
                                                project_links_directory=self.project_links_dir)

    def setUp(self):
        self._compose_delivery_service()

    def test__create_links_area_for_project_runfolders(self):
        with tempfile.TemporaryDirectory() as tmpdirname, \
                mock.patch(
                    "delivery.services.file_system_service.FileSystemService",
                    new=FileSystemService()) as file_system_service:

            # replace the symlink_preserve with symlink, which doesn't require the source file to exist
            file_system_service.symlink_preserve = FileSystemService.symlink
            self._compose_delivery_service(file_system_service=file_system_service)

            self.delivery_service.project_links_directory = tmpdirname
            batch_nbr = 1337
            project_link_area = self.delivery_service._create_links_area_for_project_runfolders("ABC_123",
                                                                                                self.runfolder_projects,
                                                                                                batch_nbr)

            project_linking_area_base = os.path.join(self.delivery_service.project_links_directory,
                                                     "ABC_123",
                                                     str(batch_nbr))

            self.assertEqual(project_link_area,
                             project_linking_area_base)

            self.assertTrue(
                os.path.islink(
                    os.path.join(
                        project_linking_area_base,
                        "160930_ST-E00216_0112_BH37CWALXX")))

            self.assertTrue(
                os.path.islink(
                    os.path.join(
                        project_linking_area_base,
                        "160930_ST-E00216_0111_BH37CWALXX")))

    def test_deliver_arbitrary_directory_project(self):

        staging_service_mock = mock.create_autospec(StagingService)
        staging_service_mock.create_new_stage_order.return_value = \
            StagingOrder(id=1,
                         source=self.general_project.path,
                         status=StagingStatus.pending,
                         staging_target='/foo/bar',
                         size=1024
                         )

        general_project_repo_mock = mock.create_autospec(GeneralProjectRepository)
        general_project_repo_mock.get_project.return_value = self.general_project

        delivery_sources_repo_mock = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
        delivery_sources_repo_mock.source_exists.return_value = False
        delivery_sources_repo_mock.create_source.return_value = DeliverySource(project_name="ABC_123",
                                                                               source_name=self.general_project.name,
                                                                               path=self.general_project.path)

        self._compose_delivery_service(general_project_repo=general_project_repo_mock,
                                       delivery_sources_repo=delivery_sources_repo_mock,
                                       staging_service=staging_service_mock)

        result = self.delivery_service.deliver_arbitrary_directory_project("ABC_123")
        self.assertTrue(result["ABC_123"] == 1)

    def test_deliver_arbitrary_directory_project_force(self):

        staging_service_mock = mock.create_autospec(StagingService)
        staging_service_mock.create_new_stage_order.return_value = \
            StagingOrder(id=1,
                         source=self.general_project.path,
                         status=StagingStatus.pending,
                         staging_target='/foo/bar',
                         size=1024
                         )

        general_project_repo_mock = mock.create_autospec(GeneralProjectRepository)
        general_project_repo_mock.get_project.return_value = self.general_project

        delivery_sources_repo_mock = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
        delivery_sources_repo_mock.source_exists.return_value = True
        delivery_sources_repo_mock.create_source.return_value = DeliverySource(project_name="ABC_123",
                                                                               source_name=self.general_project.name,
                                                                               path=self.general_project.path)

        self._compose_delivery_service(general_project_repo=general_project_repo_mock,
                                       delivery_sources_repo=delivery_sources_repo_mock,
                                       staging_service=staging_service_mock)

        with self.assertRaises(ProjectAlreadyDeliveredException):
            self.delivery_service.deliver_arbitrary_directory_project("ABC_123", force_delivery=False)

        result = self.delivery_service.deliver_arbitrary_directory_project("ABC_123", force_delivery=True)
        self.assertTrue(result["ABC_123"] == 1)

    def test_deliver_single_runfolder(self):
        staging_service_mock = mock.create_autospec(StagingService)
        staging_service_mock.create_new_stage_order.return_value = \
            StagingOrder(id=1,
                         source=self.runfolder_projects[0].path,
                         status=StagingStatus.pending,
                         staging_target='/foo/bar',
                         size=1024)
        runfolder_service_mock = mock.create_autospec(RunfolderService)

        def my_project_iterator(runfolder_name, only_these_projects):
            yield self.runfolder_projects[0]
        runfolder_service_mock.find_projects_on_runfolder = my_project_iterator

        delivery_sources_repo_mock = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
        delivery_sources_repo_mock.source_exists.return_value = False
        delivery_sources_repo_mock.create_source.return_value = \
            DeliverySource(project_name="ABC_123",
                           source_name="{}/{}".format(
                               self.runfolder_projects[0].runfolder_name,
                               self.runfolder_projects[0].name),
                           path=self.general_project.path)

        self._compose_delivery_service(runfolder_service=runfolder_service_mock,
                                       delivery_sources_repo=delivery_sources_repo_mock,
                                       staging_service=staging_service_mock)

        result = self.delivery_service.deliver_single_runfolder(runfolder_name="160930_ST-E00216_0112_BH37CWALXX",
                                                                only_these_projects=None,
                                                                force_delivery=False)
        self.assertEqual(result["ABC_123"], 1)

    def test_deliver_single_runfolder_force(self):
        staging_service_mock = mock.create_autospec(StagingService)
        staging_service_mock.create_new_stage_order.return_value = \
            StagingOrder(id=1,
                         source=self.runfolder_projects[0].path,
                         status=StagingStatus.pending,
                         staging_target='/foo/bar',
                         size=1024)
        runfolder_service_mock = mock.create_autospec(RunfolderService)

        def my_project_iterator(runfolder_name, only_these_projects):
            yield self.runfolder_projects[0]
        runfolder_service_mock.find_projects_on_runfolder = my_project_iterator

        delivery_sources_repo_mock = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
        delivery_sources_repo_mock.source_exists.return_value = True
        delivery_sources_repo_mock.create_source.return_value = \
            DeliverySource(project_name="ABC_123",
                           source_name="{}/{}".format(
                               self.runfolder_projects[0].runfolder_name,
                               self.runfolder_projects[0].name),
                           path=self.general_project.path)

        self._compose_delivery_service(runfolder_service=runfolder_service_mock,
                                       delivery_sources_repo=delivery_sources_repo_mock,
                                       staging_service=staging_service_mock)

        with self.assertRaises(ProjectAlreadyDeliveredException):
            self.delivery_service.deliver_single_runfolder(runfolder_name="160930_ST-E00216_0112_BH37CWALXX",
                                                           only_these_projects=None,
                                                           force_delivery=False)
        result = self.delivery_service.deliver_single_runfolder(runfolder_name="160930_ST-E00216_0112_BH37CWALXX",
                                                                only_these_projects=None,
                                                                force_delivery=True)
        self.assertEqual(result["ABC_123"], 1)

    def test_deliver_all_runfolders_for_project(self):
        with tempfile.TemporaryDirectory() as tmpdirname:

            staging_service_mock = mock.create_autospec(StagingService)
            staging_service_mock.create_new_stage_order.return_value = \
                StagingOrder(id=1,
                             source=os.path.join(tmpdirname, "ABC_123", "1"),
                             status=StagingStatus.pending,
                             staging_target='/foo/bar',
                             size=1024)
            runfolder_service_mock = mock.create_autospec(RunfolderService)

            def my_project_iterator(project_name):
                for proj in self.runfolder_projects:
                    yield proj
            runfolder_service_mock.find_runfolders_for_project = my_project_iterator

            file_system_service_mock = mock.create_autospec(FileSystemService)

            delivery_sources_repo_mock = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
            delivery_sources_repo_mock.source_exists.return_value = False
            delivery_sources_repo_mock.find_highest_batch_nbr.return_value = 1
            delivery_sources_repo_mock.create_source.return_value = \
                DeliverySource(project_name="ABC_123",
                               source_name="{}/{}".format(
                                   "ABC_123",
                                   "batch1"),
                               path=self.general_project.path,
                               batch=1)

            self._compose_delivery_service(runfolder_service=runfolder_service_mock,
                                           delivery_sources_repo=delivery_sources_repo_mock,
                                           staging_service=staging_service_mock,
                                           file_system_service=file_system_service_mock,
                                           project_links_dir=tmpdirname)

            projects_and_ids, projects = \
                self.delivery_service.deliver_all_runfolders_for_project(project_name="ABC_123",
                                                                         mode=DeliveryMode.CLEAN)

            self.assertEqual(projects_and_ids["ABC_123"], 1)

            staged_runfolders = list(map(lambda staged_path: staged_path.to_dict(), projects))
            paths = []
            for item in staged_runfolders:
                paths.append(item['path'])
            self.assertEqual(paths,["/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/ABC_123",
                                    "/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123"])


if __name__ == '__main__':
    unittest.main()
