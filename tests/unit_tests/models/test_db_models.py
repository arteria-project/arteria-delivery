import unittest

from delivery.models.db_models import DeliveryOrder

class TestDeliveryOrder(unittest.TesCase):
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
