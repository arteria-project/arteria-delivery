

import unittest


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from delivery.models.db_models import SQLAlchemyBase, DeliveryOrder, DeliveryStatus
from delivery.repositories.deliveries_repository import DatabaseBasedDeliveriesRepository


class TestDeliveryRepository(unittest.TestCase):

    def setUp(self):
        # NOTE setting echo to true is very useful to se which sql statements get
        # executed, but since it fills up the logs a lot it's been disabled by
        # default here.
        engine = create_engine('sqlite:///:memory:', echo=False)
        SQLAlchemyBase.metadata.create_all(engine)

        # Throw some data into the in-memory db
        session_factory = sessionmaker()
        session_factory.configure(bind=engine)

        self.session = session_factory()

        self.delivery_order_1 = DeliveryOrder(delivery_source='/foo/source',
                                              delivery_project='bar',
                                              delivery_status=DeliveryStatus.pending,
                                              staging_order_id=1)

        self.session.add(self.delivery_order_1)

        self.session.commit()

        # Prep the repo
        self.delivery_repo = DatabaseBasedDeliveriesRepository(session_factory)

    def test_get_delivery_orders_for_source(self):
        actual = self.delivery_repo.get_delivery_orders_for_source('/foo/source')
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual[0].id, self.delivery_order_1.id)

    def test_get_delivery_order_by_id(self):
        actual = self.delivery_repo.get_delivery_order_by_id(1)
        self.assertEqual(actual.id, self.delivery_order_1.id)

    def test_get_delivery_orders(self):
        actual = self.delivery_repo.get_delivery_orders()
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual[0].id, self.delivery_order_1.id)

    def test_create_delivery_order(self):

        actual = self.delivery_repo.create_delivery_order(delivery_source='/foo/source2',
                                                          delivery_project='snpseq00001',
                                                          delivery_status=DeliveryStatus.pending,
                                                          staging_order_id=2)

        self.assertEqual(actual.id, 2)
        self.assertEqual(actual.delivery_source, '/foo/source2')
        self.assertEqual(actual.delivery_project, 'snpseq00001')
        self.assertEqual(actual.delivery_status, DeliveryStatus.pending)
        self.assertEqual(actual.staging_order_id, 2)

        # Check that the object has been committed, i.e. there are no 'dirty' objects in session
        self.assertEqual(len(self.session.dirty), 0)
        order_from_session = self.session.query(DeliveryOrder).filter(DeliveryOrder.id == actual.id).one()
        self.assertEqual(order_from_session.id, actual.id)
