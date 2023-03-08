import datetime

from delivery.models.db_models import DDSDelivery, DDSPut, DeliveryStatus


class DatabaseBasedDDSRepository:
    def __init__(self, session_factory):
        """
        Instantiate a new DatabaseBasedDDSRepository
        :param session_factory: a factory method that can create a new
        sqlalchemy Session object.
        """
        self.session = session_factory()

    def _get_row(self, primary_key):
        raise NotImplementedError

    def set_to_completed(self, primary_key, date_completed=None):
        if not date_completed:
            date_completed = datetime.datetime.now()

        row = self._get_row(primary_key)

        row.delivery_status = DeliveryStatus.delivery_successful
        row.date_completed = date_completed

        self.session.commit()

    def update_status(self, primary_key, new_status):
        row = self._get_row(primary_key)
        row.delivery_status = new_status
        self.session.commit()


class DatabaseBasedDDSDeliveryRepository(DatabaseBasedDDSRepository):
    """
    Class to manipulate the DDSDelivery database.
    """
    def _get_row(self, primary_key):
        return self.session.get(DDSDelivery, primary_key)

    def register_dds_delivery(
            self,
            dds_project_id,
            ngi_project_name,
            date_started=None,
            delivery_status=None,
    ):
        if not date_started:
            date_started = datetime.datetime.now()

        if not delivery_status:
            delivery_status = DeliveryStatus.delivery_in_progress

        dds_delivery = DDSDelivery(
            dds_project_id=dds_project_id,
            ngi_project_name=ngi_project_name,
            date_started=date_started,
            delivery_status=delivery_status,
            )

        self.session.add(dds_delivery)
        self.session.commit()

        return dds_delivery

    def get_dds_delivery(self, dds_project_id):
        return self._get_row(dds_project_id)


class DatabaseBasedDDSPutRepository(DatabaseBasedDDSRepository):
    def _get_row(self, primary_key):
        return self.session.get(DDSPut, primary_key)

    def register_dds_put(
            self,
            dds_project_id,
            dds_pid,
            delivery_source,
            delivery_path,
            destination=None,
            date_started=None,
            delivery_status=None,
    ):
        if not date_started:
            date_started = datetime.datetime.now()

        if not delivery_status:
            delivery_status = DeliveryStatus.delivery_in_progress

        dds_put = DDSPut(
            dds_project_id=dds_project_id,
            dds_pid=dds_pid,
            delivery_source=delivery_source,
            delivery_path=delivery_path,
            destination=destination,
            date_started=date_started,
            delivery_status=delivery_status,
            )

        self.session.add(dds_put)
        self.session.commit()

        return dds_put

    def get_dds_put(self, row_id):
        return self._get_row(row_id)
