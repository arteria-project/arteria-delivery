import datetime

from delivery.models.db_models import DDSDelivery, DDSPut, DeliveryStatus


class DatabaseBasedDDSRepository:
    """
    Base class for DDSDelivery and DDSPut
    """
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
        """
        Set status to `delivery_successful` and set date of completion.

        Parameters
        ----------
        primary_key: str
            id to row in database
        date_completed: datetime
            date of completion, if None, the current time and date will be
            used.
        """
        if not date_completed:
            date_completed = datetime.datetime.now()

        row = self._get_row(primary_key)

        row.delivery_status = DeliveryStatus.delivery_successful
        row.date_completed = date_completed

        self.session.commit()

    def update_status(self, primary_key, new_status):
        """
        Update status of the given delivery

        Parameters
        ----------
        primary_key: str
            id to row in database
        new_status: DeliveryStatus
            new delivery status
        """
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
        """
        Add a new delivery to the database. By default, starting date will be
        set to the current date and delivery status will be set to
        `delivery_in_progress`.

        Parameters
        ----------
        dds_project_id: str (e.g. snpseq00000)
            project id provided by dds, must be unique
        ngi_project_name: str (e.g. AB-1234)
            name of the project (typically project name in Clarity)
        date_started: datetime
            date when the delivery was started
        delivery_status: DeliveryStatus
            Initial delivery status

        Returns
        -------
        DDSDelivery
        """
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
        """
        Get a delivery by dds project id from the database

        Parameters
        ----------
        dds_project_id: str (e.g. snpseq00000)
            id of the project

        Returns
        -------
        DDSDelivery
        """
        return self._get_row(dds_project_id)


class DatabaseBasedDDSPutRepository(DatabaseBasedDDSRepository):
    """
    Class to manipulate the DDSPut database.
    """
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
        """
        Register a new upload in the database. By default, starting date will
        be set to the current date and delivery status will be set to
        `delivery_in_progress`.

        Parameters
        ----------
        dds_project_id: str (e.g. snpseq00000)
            id of the project the data is uploaded to
        dds_pid: int
            id of the dds process uploading the data
        delivery_source: str
            unique identifier to the folder being uploaded (e.g. project name
            or runfolder name). This is used to avoid delivering the same item
            twice.
        delivery_path: str
            path to the data being uploaded
        destination: str
            path where to upload the data in the dds project
        date_started: datetime
            date when the upload was started (by default, will use the current
            time)
        delivery_status: DeliveryStatus
            Initial status of the upload

        Returns
        -------
        DDSPut
        """
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

    def get_dds_put(self, primary_key):
        """
        Return a dds put entry from the database

        Parameters
        ----------
        primary_key: int
            id of the entry

        Returns
        -------
        DDSPut
        """
        return self._get_row(primary_key)

    def was_delivered_before(self, ngi_project_name, source):
        """
        Returns if a source belonging to a specific project has been delivered
        before.

        Parameters
        ----------
        ngi_project_name: str (e.g. AB-1234)
            project name
        source: str
            unique identifier to the folder being uploaded (e.g. project name
            or runfolder name). This is used to avoid delivering the same item
            twice.

        Returns
        -------
        bool
        """
        return self.session.query(DDSPut) \
            .join(DDSDelivery) \
            .filter(DDSDelivery.ngi_project_name == ngi_project_name) \
            .filter(DDSPut.delivery_source == source) \
            .first() is not None

    def get_dds_put_by_status(self, dds_project_id, delivery_status):
        """
        Returns all uploads to a specific project with the given status

        Parameters
        ----------
        dds_project_id: str (e.g. snpseq00000)
            dds project where the uploads were made
        delivery_status: DeliveryStatus
            status to filter by

        Returns
        -------
        [DDSPut]
        """
        return self.session.query(DDSPut) \
            .join(DDSDelivery) \
            .filter(DDSDelivery.dds_project_id == dds_project_id) \
            .filter(DDSPut.delivery_status == delivery_status) \
            .all()
