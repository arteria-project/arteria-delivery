import unittest

from delivery.models.db_models import DeliveryOrder
from delivery.models.db_models import StagingOrder, StagingStatus

class TestDeliveryOrder(unittest.TestCase):
    def test_is_dds(self):
        mover_order = DeliveryOrder(
                delivery_source="/foo/bar",
                delivery_project="delivery123456",
                )

        dds_order = DeliveryOrder(
                delivery_source="/foo/bar",
                delivery_project="snpseq00001",
                )

        self.assertFalse(mover_order.is_dds())
        self.assertTrue(dds_order.is_dds())

class TestStagingOrder(unittest.TestCase):
    def test_get_staging_path(self):
        staging_order = StagingOrder(
                source='/staging/source/data',
                staging_target='/staging/target/data',
                status=StagingStatus.pending,
                )

        self.assertEquals(staging_order.get_staging_path(), '/staging/target/data')
