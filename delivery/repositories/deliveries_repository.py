
from sqlalchemy.orm.exc import NoResultFound

from delivery.models.db_models import DeliveryOrder


class DatabaseBasedDeliveriesRepository(object):
    """
    Creates database deliveries and stores theme in the backing database. Can also return objects
    from the database given different factors.
    """

    def __init__(self, session_factory):
        """
        Instantiate a new DatabaseBasedDeliveriesRepository
        :param session_factory: a factory method that can create a new sqlalchemy Session object.
        """
        self.session = session_factory()

    def get_delivery_orders_for_source(self, source_directory):
        """
        Returns all delivery orders which match the given source directory
        :param source_directory: to search for
        :return: all matching delivery orders as a list.
        """
        return self.session.query(DeliveryOrder).filter(DeliveryOrder.delivery_source == source_directory).all()

    def get_delivery_order_by_id(self, delivery_order_id, custom_session=None):
        """
        Get the delivery order matching the given id
        The custom_session used here is used because in the `DeliveryService` it is necessary to
        make this query from a separate thread, something which sqlalchemy does not allow. Therefore
        I've made it possible to provide a separate session here (in that case a new session instantiate in the
        thread), while hacky it appears to work. /JD 20161212
        :param delivery_order_id: to search for
        :param custom_session: provide an other session object if that is necessary for your use case.
        :return: the matching delivery order, or None, if no order was found matching id
        """
        if custom_session:
            session = custom_session
        else:
            session = self.session
        try:
            return session.query(DeliveryOrder).filter(DeliveryOrder.id == delivery_order_id).one()
        except NoResultFound:
            return None

    def get_delivery_orders(self):
        """
        Return all delivery orders for the database as a list
        :return:
        """
        return self.session.query(DeliveryOrder).all()

    def create_delivery_order(self,
                              delivery_source,
                              delivery_project,
                              delivery_status,
                              staging_order_id,
                              md5sum_file=None):
        """
        Create a new delivery order and commit it to the database
        :param delivery_source: the source directory to be delivered
        :param delivery_project: the project code for the project to deliver to
        :param delivery_status: status of the delivery
        :param staging_order_id: NOTA BENE: this will need to be verified against the staging table before
                                 inserting it here, because at this point there is no validation that the
                                 value is valid!
        :param md5sum_file: Optional path to an md5sum file that mover to check files against.
        :return: the created delivery order
        """
        order = DeliveryOrder(delivery_source=delivery_source,
                              delivery_project=delivery_project,
                              delivery_status=delivery_status,
                              staging_order_id=staging_order_id,
                              md5sum_file=md5sum_file)
        self.session.add(order)
        self.session.commit()

        return order
