
import os
import json
import logging
import tempfile

from tornado.gen import coroutine

from delivery.handlers import *
from delivery.handlers.utility_handlers import ArteriaDeliveryBaseHandler
from delivery.models.project import DDSProject

log = logging.getLogger(__name__)

class DeliverByStageIdHandler(ArteriaDeliveryBaseHandler):
    """
    Handler for starting deliveries based on a previously staged directory/file
    # TODO This is still work in progress
    """

    def initialize(self, **kwargs):
        self.delivery_service = kwargs["dds_service"]
        super(DeliverByStageIdHandler, self).initialize(kwargs)

    @coroutine
    def post(self, staging_id):
        required_members = [
                "delivery_project_id",
                "auth_token",
                ]
        request_data = self.body_as_object(required_members=required_members)

        delivery_project_id = request_data["delivery_project_id"]
        auth_token = request_data["auth_token"]
        deadline = request_data.get("deadline")
        release = request_data.get("release", True)
        email = request_data.get("email", True)

        # This should only be used for testing purposes /JD 20170202
        skip_delivery_request = request_data.get("skip_delivery")
        if skip_delivery_request and skip_delivery_request == True:
            log.info("Got the command to skip delivery...")
            skip_delivery = True
        else:
            log.debug("Will not skip running delivery!")
            skip_delivery = False

        dds_project = DDSProject(
                self.delivery_service,
                auth_token,
                delivery_project_id)

        delivery_id = yield dds_project.deliver(
                staging_id,
                skip_delivery=skip_delivery,
                deadline=deadline,
                release=release,
                email=email,
                )

        status_end_point = "{0}://{1}{2}".format(
                self.request.protocol,
                self.request.host,
                self.reverse_url("delivery_status", delivery_id))

        self.set_status(ACCEPTED)
        self.write_json({'delivery_order_id': delivery_id,
                         'delivery_order_link': status_end_point})


class DeliveryStatusHandler(ArteriaDeliveryBaseHandler):

    def initialize(self, **kwargs):
        self.delivery_service = kwargs["dds_service"]
        super(DeliveryStatusHandler, self).initialize(kwargs)

    @coroutine
    def get(self, delivery_order_id):
        delivery_order = self.delivery_service\
            .get_delivery_order_by_id(delivery_order_id)

        delivery_order = yield self.delivery_service.update_delivery_status(
                delivery_order_id)

        body = {
                'id': delivery_order.id,
                'status': delivery_order.delivery_status.name,
                }

        self.write_json(body)
        self.set_status(OK)
