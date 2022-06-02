import unittest

from delivery.models.db_models import StagingOrder, StagingStatus


class TestStagingOrder(unittest.TestCase):
    def test_get_staging_path(self):
        staging_order = StagingOrder(
                source='/staging/source/data',
                staging_target='/staging/target/data',
                status=StagingStatus.pending,
                )

        self.assertEquals(
                staging_order.get_staging_path(),
                '/staging/target/data')
