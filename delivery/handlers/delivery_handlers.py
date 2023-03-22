
import os
import json
import logging
import tempfile

from tornado.gen import coroutine

from delivery.handlers import *
from delivery.handlers.utility_handlers import ArteriaDeliveryBaseHandler
from delivery.models.project import DDSProject

log = logging.getLogger(__name__)


class DeliverProjectHandler(ArteriaDeliveryBaseHandler):
    """
    Handler for delivering a project a project (i.e. a directory placed in the
    directory defined by the arteria delivery service
    `general_project_directory` configuration).
    """
    def initialize(self, **kwargs):
        self.dds_service = kwargs["dds_service"]
        self.general_project_repo = kwargs["general_project_repo"]
        super().initialize(kwargs)

    async def post(self, project_name):
        required_members = [
            "auth_token",
            "pi",
            "description",
        ]

        request_data = self.body_as_object(
            required_members=required_members)

        project_metadata = {
            key: request_data[key]
            for key in [
                "pi",
                "description",
                "owners",
                "researchers",
                "project_alias",
            ]
            if key in request_data
        }

        force_delivery = request_data.get("force_delivery", False)

        if (
                self.dds_service.dds_put_repo
                .was_delivered_before(project_name, project_name)
                and not force_delivery
        ):
            self.set_status(
                FORBIDDEN,
                f"The project {project_name} has already been delivered. "
                "Use the force to bypass and deliver anyway."
            )
            return

        project_path = self.general_project_repo.get_project(
            project_metadata.get("project_alias", project_name)).path

        dds_project = await DDSProject.new(
            project_name,
            project_metadata,
            request_data["auth_token"],
            self.dds_service)

        log.info(
            f"New dds project created for project {project_name} "
            f"with id {dds_project.project_id}"
        )

        self.set_status(ACCEPTED)
        self.write_json({
            'dds_project_id': dds_project.project_id,
            'status_link': "{0}://{1}{2}".format(
                self.request.protocol,
                self.request.host,
                self.reverse_url("delivery_status", dds_project.project_id)
            )
        })
        self.finish()

        await dds_project.put(project_name, project_path)
        log.info(f"Uploaded {project_path} to {dds_project.project_id}")

        if request_data.get("release"):
            await dds_project.release(
                deadline=request_data.get("deadline", None),
                email=request_data.get("email", True),
            )
            log.info(f"Released project {dds_project.project_id}")

        dds_project.complete()


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
