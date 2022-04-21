import os.path
import logging
import re
import json
from tornado import gen

from delivery.models.db_models import StagingStatus, DeliveryStatus
from delivery.exceptions import ProjectNotFoundException, TooManyProjectsFound, InvalidStatusException

log = logging.getLogger(__name__)


class DDSService(object):

    def __init__(self, external_program_service, staging_service, delivery_repo, session_factory, dds_conf):
        self.external_program_service = external_program_service
        self.mover_external_program_service = self.external_program_service
        self.staging_service = staging_service
        self.delivery_repo = delivery_repo
        self.session_factory = session_factory
        self.dds_conf = dds_conf

    @staticmethod
    @gen.coroutine
    def _run_mover(delivery_order_id, delivery_order_repo, external_program_service, session_factory, dds_conf):
        session = session_factory()

        # This is a somewhat hacky work-around to the problem that objects created in one
        # thread, and thus associated with another session cannot be accessed by another
        # thread, therefore it is re-materialized in here...
        delivery_order = delivery_order_repo.get_delivery_order_by_id(delivery_order_id, session)
        try:
            cmd = [
                    'dds',
                    '-tp', dds_conf["token_path"],
                    '-l', dds_conf["log_path"],
                    'data', 'put',
                    '--source', delivery_order.delivery_source,
                    '-p', delivery_order.dds_project_id,
                    '--silent',
                    ]

            log.debug("Running dds with cmd: {}".format(" ".join(cmd)))

            execution = external_program_service.run(cmd)
            delivery_order.delivery_status = DeliveryStatus.delivery_in_progress
            delivery_order.mover_pid = execution.pid
            session.commit()

            execution_result = yield external_program_service.wait_for_execution(execution)

            if execution_result.status_code == 0:
                delivery_order.delivery_status = DeliveryStatus.delivery_successful
                log.info(f"Successfully delivered: {delivery_order}")
            else:
                delivery_order.delivery_status = DeliveryStatus.delivery_failed
                error_msg = f"Failed to deliver: {delivery_order}. DDS returned status code: {execution_result.status_code}"
                log.error(error_msg)
                raise RuntimeError(error_msg)

        except Exception as e:
            delivery_order.delivery_status = DeliveryStatus.delivery_failed
            log.error(f"Failed to deliver: {delivery_order} because this exception was logged: {e}")
            raise e
        finally:
            # Always commit the state change to the database
            session.commit()

    @staticmethod
    @gen.coroutine
    def _get_dds_project_id(delivery_project, external_program_service):
        cmd = ['dds', 'project', 'ls', '--json']
        execution = external_program_service.run(cmd)
        execution_result = yield external_program_service.wait_for_execution(execution)

        if execution_result.status_code == 0:
            projects = [project
                    for project in json.loads(execution_result.stdout)
                    if project['Title'] == delivery_project]

            if len(projects) == 1:
                project_id = projects[0]['Project ID']
                log.info(f"Fetched DDS project id for project {delivery_project}: {project_id}")
                return project_id
            elif len(projects) == 0:
                error_msg = f"Project {delivery_project} could not be found in DDS."
                log.error(error_msg)
                raise ProjectNotFoundException(error_msg)
            else:
                error_msg = f"Multiple projects found with name {delivery_project}."
                log.error(error_msg)
                raise TooManyProjectsFound(error_msg)

        else:
            error_msg = f"Project {delivery_project} could not be found in DDS. DDS returned status code: {execution_result.status_code}, DDS stderr: {execution_result.stderr}"
            log.error(error_msg)
            raise ProjectNotFoundException(error_msg)

    @gen.coroutine
    def deliver_by_staging_id(self, staging_id, delivery_project, md5sum_file, skip_mover=False):

        stage_order = self.staging_service.get_stage_order_by_id(staging_id)
        if not stage_order or not stage_order.status == StagingStatus.staging_successful:
            raise InvalidStatusException("Only deliver by staging_id if it has a successful status!"
                                         "Staging order was: {}".format(stage_order))

        if not skip_mover:
            dds_project_id = yield DDSService._get_dds_project_id(delivery_project, self.mover_external_program_service)
        else:
            dds_project_id = None

        delivery_order = self.delivery_repo.create_delivery_order(delivery_source=stage_order.get_staging_path(),
                                                                  delivery_project=delivery_project,
                                                                  delivery_status=DeliveryStatus.pending,
                                                                  staging_order_id=staging_id,
                                                                  dds_project_id=dds_project_id,
                                                                  md5sum_file=md5sum_file)

        args_for_run_mover = {'delivery_order_id': delivery_order.id,
                              'delivery_order_repo': self.delivery_repo,
                              'external_program_service': self.mover_external_program_service,
                              'session_factory': self.session_factory,
                              'dds_conf': self.dds_conf
                              }

        if skip_mover:
            session = self.session_factory()
            delivery_order.delivery_status = DeliveryStatus.delivery_skipped
            session.commit()
        else:
            yield DDSService._run_mover(**args_for_run_mover)

        return delivery_order.id

    def get_delivery_order_by_id(self, delivery_order_id):
        return self.delivery_repo.get_delivery_order_by_id(delivery_order_id)

    @gen.coroutine
    def update_delivery_status(self, delivery_order_id):
        """
        Check delivery status and update the delivery database accordingly
        """
        # NB: this is done automatically with the new DDS implementation now.
        return self.get_delivery_order_by_id(delivery_order_id)
