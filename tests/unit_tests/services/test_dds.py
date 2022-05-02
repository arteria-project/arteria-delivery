
import random
from mock import MagicMock, create_autospec, patch

from tornado.testing import AsyncTestCase, gen_test
from tornado.gen import coroutine

from delivery.services.external_program_service import ExternalProgramService
from delivery.services.dds_service import DDSService
from delivery.models.db_models import DeliveryOrder, StagingOrder, StagingStatus, DeliveryStatus
from delivery.models.execution import ExecutionResult, Execution
from delivery.exceptions import InvalidStatusException, CannotParseMoverOutputException

from tests.test_utils import MockIOLoop, assert_eventually_equals


class TestDDSService(AsyncTestCase):

    def setUp(self):

        example_dds_project_ls_stdout = """[
  {
    "Last updated": "Thu, 03 Mar 2022 11:46:31 CET",
    "PI": "Dungeon master",
    "Project ID": "snpseq00001",
    "Size": 26956752654,
    "Status": "In Progress",
    "Title": "Bullywug anatomy"
  },
  {
    "Last updated": "Thu, 03 Mar 2022 10:34:05 CET",
    "PI": "matas618",
    "Project ID": "snpseq00002",
    "Size": 0,
    "Status": "In Progress",
    "Title": "Site admins project"
  }
]
        """


        self.mock_mover_runner = create_autospec(ExternalProgramService)
        mock_process = MagicMock()
        mock_execution = Execution(pid=random.randint(1, 1000), process_obj=mock_process)
        self.mock_mover_runner.run.return_value = mock_execution

        @coroutine
        def wait_as_coroutine(x):
            return ExecutionResult(stdout=example_dds_project_ls_stdout, stderr="", status_code=0)

        self.mock_mover_runner.wait_for_execution = wait_as_coroutine


        self.mock_staging_service = MagicMock()
        self.mock_delivery_repo = MagicMock()

        self.delivery_order = DeliveryOrder(
                id=1,
                delivery_source="/staging/dir/bar",
                delivery_project="snpseq00001",
                )

        self.mock_delivery_repo.create_delivery_order.return_value = self.delivery_order
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = self.delivery_order

        self.mock_session_factory = MagicMock()
        self.mock_dds_config = {'token_path': '/foo/bar/auth', 'log_path': '/foo/bar/log'}
        self.dds_service = DDSService(
                external_program_service=None,
                staging_service=self.mock_staging_service,
                delivery_repo=self.mock_delivery_repo,
                session_factory=self.mock_session_factory,
                dds_conf=self.mock_dds_config
                )

        # Inject separate external runner instances for the tests, since they need to return
        # different information
        self.dds_service.mover_external_program_service = self.mock_mover_runner

        super(TestDDSService, self).setUp()

    @gen_test
    def test_deliver_by_staging_id(self):
        source = '/foo/bar'
        staging_target = '/staging/dir/bar'
        staging_order = StagingOrder(source=source, staging_target=staging_target)
        staging_order.status = StagingStatus.staging_successful
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        self.mock_staging_service.get_delivery_order_by_id.return_value = self.delivery_order

        with patch('shutil.rmtree') as mock_rmtree:
            res = yield self.dds_service.deliver_by_staging_id(
                    staging_id=1,
                    delivery_project='snpseq00001',
                    token_path='token_path',
                    md5sum_file='md5sum_file')
            mock_rmtree.assert_called_with(staging_target)

        def _get_delivery_order():
            return self.delivery_order.delivery_status
        assert_eventually_equals(self, 1, _get_delivery_order, DeliveryStatus.delivery_successful)
        self.mock_mover_runner.run.assert_called_with(['dds', '-tp', 'token_path', '-l', '/foo/bar/log', 'data', 'put', '--source', '/foo', '-p', 'snpseq00001', '--silent'])

    @gen_test
    def test_deliver_by_staging_id_raises_on_non_existent_stage_id(self):
        self.mock_staging_service.get_stage_order_by_id.return_value = None

        with self.assertRaises(InvalidStatusException):

            yield self.dds_service.deliver_by_staging_id(
                    staging_id=1,
                    delivery_project='snpseq00001',
                    md5sum_file='md5sum_file',
                    token_path='token_path',
                    )

    @gen_test
    def test_deliver_by_staging_id_raises_on_non_successful_stage_id(self):

        staging_order = StagingOrder()
        staging_order.status = StagingStatus.staging_failed
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        with self.assertRaises(InvalidStatusException):

            yield self.dds_service.deliver_by_staging_id(
                    staging_id=1,
                    delivery_project='snpseq00001',
                    md5sum_file='md5sum_file',
                    token_path='token_path',
                    )

    def test_delivery_order_by_id(self):
        delivery_order = DeliveryOrder(id=1,
                                       delivery_source='src',
                                       delivery_project='snpseq00001',
                                       delivery_status=DeliveryStatus.delivery_in_progress,
                                       staging_order_id=11,
                                       md5sum_file='file')
        self.mock_delivery_repo.get_delivery_order_by_id.return_value = delivery_order
        actual = self.dds_service.get_delivery_order_by_id(1)
        self.assertEqual(actual.id, 1)

    def test_possible_to_delivery_by_staging_id_and_skip_mover(self):

        staging_order = StagingOrder(source='/foo/bar', staging_target='/staging/dir/bar')
        staging_order.status = StagingStatus.staging_successful
        self.mock_staging_service.get_stage_order_by_id.return_value = staging_order

        self.mock_staging_service.get_delivery_order_by_id.return_value = self.delivery_order

        self.dds_service.deliver_by_staging_id(
                staging_id=1,
                delivery_project='snpseq00001',
                md5sum_file='md5sum_file',
                token_path='token_path',
                skip_mover=True,
                )

        def _get_delivery_order():
            return self.delivery_order.delivery_status
        assert_eventually_equals(self, 1, _get_delivery_order, DeliveryStatus.delivery_skipped)
