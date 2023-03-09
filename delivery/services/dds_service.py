import logging
from tornado import gen


log = logging.getLogger(__name__)


class DDSService(object):
    def __init__(
            self,
            external_program_service,
            staging_service,
            staging_dir,
            delivery_repo,
            dds_delivery_repo,
            dds_put_repo,
            session_factory,
            dds_conf):
        self.external_program_service = external_program_service
        self.dds_external_program_service = self.external_program_service
        self.staging_service = staging_service
        self.staging_dir = staging_dir
        # `delivery_repo` and `dds_delivery_repo` have similar names here but
        # `delivery_repo` will be removed once we decommission the old API
        # version /AC 2023-03-09
        self.delivery_repo = delivery_repo
        self.dds_delivery_repo = dds_delivery_repo
        self.dds_put_repo = dds_put_repo
        self.session_factory = session_factory
        self.dds_conf = dds_conf

    def get_delivery_order_by_id(self, delivery_order_id):
        return self.delivery_repo.get_delivery_order_by_id(delivery_order_id)

    @gen.coroutine
    def update_delivery_status(self, delivery_order_id):
        """
        Check delivery status and update the delivery database accordingly
        """
        # NB: this is done automatically with the new DDS implementation now.
        return self.get_delivery_order_by_id(delivery_order_id)
