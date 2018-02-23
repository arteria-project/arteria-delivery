import unittest
import datetime



from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from delivery.models.db_models import SQLAlchemyBase, NGIStatus, NGIStatusAsEnum
from delivery.repositories.ngi_status_repository import DatabaseBasedNGIStatusRepository

class TestDatabaseBasedNGIStatusRepository(unittest.TestCase):

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

        self.ngi_status_1 = NGIStatus(order_id=1,
                               delivery_project='delivery0001',
                               status='pending',
                               timestamp=datetime.datetime(2018, 1, 1, 11, 1, 11, 191295))

        self.ngi_status_2 = NGIStatus(order_id=2,
                               delivery_project='delivery0002',
                               status='pending',
                               timestamp=datetime.datetime(2018, 2, 2, 12, 2, 22, 191295))

        self.session.add(self.ngi_status_1)
        self.session.add(self.ngi_status_2)

        self.session.commit()

        # Prep the repo
        self.ngi_status_repo = DatabaseBasedNGIStatusRepository(session_factory)

    def test_get_status_by_order_id(self):
        actual = self.ngi_status_repo.get_status_by_order_id(1)
        self.assertEqual(actual.order_id, self.ngi_status_1.order_id)

    def test_non_valid_order_id(self):
        self.assertIsNone(self.ngi_status_repo.get_status_by_order_id(6))

    def test_set_status_by_order_id(self):
        actual = self.ngi_status_repo.set_status_by_order_id(order_id=3,
                                                    delivery_project='delivery0003',
                                                    ngi_status='pending',
                                                    date_time=datetime.datetime(2018, 3, 3, 13, 3, 33, 191295))

        self.assertEqual(actual.order_id, 3)
        self.assertEqual(actual.delivery_project, 'delivery0003')
        self.assertEqual(actual.status, NGIStatusAsEnum.pending)
        self.assertEqual(actual.timestamp, datetime.datetime(2018, 3, 3, 13, 3, 33, 191295))

    def test_setting_already_existing_id(self):
        with self.assertRaises(IntegrityError):
            self.ngi_status_repo.set_status_by_order_id(order_id=1,
                                                   delivery_project='delivery000X',
                                                   ngi_status='pending',
                                                   date_time=datetime.datetime.now())


    def test_update_status(self):
        new_status = NGIStatusAsEnum.successful
        self.ngi_status_repo.update_status(ngi_status_object=self.ngi_status_2,new_status=new_status)
        self.assertEqual(self.ngi_status_2.status, NGIStatusAsEnum.successful)


    def test_get_status_for_specific_timestamp(self):
        #Test with timestamp where older objects are available in database.
        time_stamp_1 = datetime.datetime(2018, 1, 1, 12, 1, 11, 191295)
        object_list_1 = self.ngi_status_repo.get_status_for_specific_timestamp(time_stamp=time_stamp_1)
        object_list_2 = [self.ngi_status_1]
        self.assertEqual(object_list_1,object_list_2)

        #Test with timestamp where no older objects available in database.
        time_stamp_2 = datetime.datetime(2018, 1, 1, 10, 1, 11, 191295)
        self.assertEqual(self.ngi_status_repo.get_status_for_specific_timestamp(time_stamp=time_stamp_2),[])
