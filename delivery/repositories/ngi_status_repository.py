from sqlalchemy.orm.exc import NoResultFound
from delivery.models.db_models import NGIStatus

class DatabaseBasedNGIStatusRepository(object):
    """
    Creates database objects for storing ngi-status for a delivery order in the backing database.
    Can also return and update status for objects from the database given different factors.
    """

    def __init__(self, session_factory):
        """
        Instantiate a new DatabaseBasedNGIStatusRepository
        :param session_factory: a factory method that can create a new sqlalchemy Session object.
        """
        self.session = session_factory()

    def get_status_by_order_id(self, order_id):
        """
        Get the ngi-status matching the given delivery order id
        :param order_id: delivery order id to search for
        :return: ngi-status from the matching delivery order, or None, if no order was found matching order id
        """
        try:
            return self.session.query(NGIStatus).filter(NGIStatus.order_id == order_id).one()
        except NoResultFound:
            return None


    def set_status_by_order_id(self, order_id, delivery_project, ngi_status, date_time):
        """
        Create a new ngi-status object and commit it to the database
        :param order_id: delivery order id to search for
        :param delivery_project: the project code for the project to deliver to
        :param ngi_status: ngi-status of the delivery
        :param date_time: timestamp for when status was set
        :return: the created ngi-status object
        """

        set_status = NGIStatus(order_id=order_id,
                               delivery_project=delivery_project,
                               status=ngi_status,
                               timestamp=date_time)

        self.session.add(set_status)
        self.session.commit()

        return set_status

    def update_status(self,ngi_status_object,new_status):
        """
        Update ngi-staus and commit it to the database
        :param ngi_status_object: a ngi-status database object
        :param new_status: new ngi-status for the given database object
        :return: new ngi-status to database
        """
        ngi_status_object.status = new_status

        self.session.commit()

    def get_status_for_specific_timestamp(self,time_stamp):
        """
        Select ongoing deliveries, i.e. deliveries with ngi-status pending, older than a given date.
        :param time_stamp: Datetime object e.g. datetime.datetime(2018, 2, 20, 11, 1, 11, 191295)
        :return: list of ngi-status database objects
        """

        try:
            return self.session.query(NGIStatus).filter(NGIStatus.status == 'pending')\
                .filter(NGIStatus.timestamp < time_stamp).all()
        except NoResultFound:
            return None
