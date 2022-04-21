import os.path
import logging
import re
from tornado import gen

from delivery.exceptions import InvalidStatusException, CannotParseMoverOutputException
from delivery.models.db_models import StagingStatus, DeliveryStatus

log = logging.getLogger(__name__)


class MoverDeliveryService(object):

    def __init__(self, external_program_service, staging_service, delivery_repo, session_factory, path_to_mover):
        self.external_program_service = external_program_service
        self.mover_external_program_service = self.external_program_service
        self.moverinfo_external_program_service = self.external_program_service
        self.staging_service = staging_service
        self.delivery_repo = delivery_repo
        self.session_factory = session_factory
        self.path_to_mover = path_to_mover

    @staticmethod
    def _parse_mover_id_from_mover_output(mover_output):
        log.debug('Mover output was: {}'.format(mover_output))
        pattern = re.compile('^(.+-\w+-\d+)$')
        hits = pattern.match(mover_output)
        if hits:
            return hits.group(1)
        else:
            raise CannotParseMoverOutputException("Could not parse mover id from: {}".format(mover_output))

    @staticmethod
    @gen.coroutine
    def _run_mover(delivery_order_id, delivery_order_repo, external_program_service, session_factory, path_to_mover):
        session = session_factory()

        # This is a somewhat hacky work-around to the problem that objects created in one
        # thread, and thus associated with another session cannot be accessed by another
        # thread, there fore it is re-materialized in here...
        delivery_order = delivery_order_repo.get_delivery_order_by_id(delivery_order_id, session)
        try:

            cmd = [os.path.join(path_to_mover, 'to_outbox'),
                   delivery_order.delivery_source,
                   delivery_order.delivery_project]

            if delivery_order.md5sum_file:
                cmd += delivery_order.md5sum_file

            log.debug("Running mover with cmd: {}".format(" ".join(cmd)))

            execution = external_program_service.run(cmd)
            delivery_order.delivery_status = DeliveryStatus.mover_processing_delivery
            delivery_order.mover_pid = execution.pid
            session.commit()

            execution_result = yield external_program_service.wait_for_execution(execution)

            if execution_result.status_code == 0:
                delivery_order.delivery_status = DeliveryStatus.delivery_in_progress
                delivery_order.mover_delivery_id = MoverDeliveryService.\
                    _parse_mover_id_from_mover_output(execution_result.stdout)
                log.info("Successfully started delivery with Mover of: {}".format(delivery_order))
            else:
                delivery_order.delivery_status = DeliveryStatus.mover_failed_delivery
                log.info("Failed to start Mover delivery: {}. Mover returned status code: {}".
                         format(delivery_order, execution_result.status_code))

        # TODO Better exception handling here...
        except Exception as e:
            delivery_order.delivery_status = DeliveryStatus.delivery_failed
            log.info("Failed in starting delivery: {} because this exception was logged: {}".
                     format(delivery_order, e))
        finally:
            # Always commit the state change to the database
            session.commit()

    @gen.coroutine
    def deliver_by_staging_id(self, staging_id, delivery_project, md5sum_file, skip_mover=False):

        stage_order = self.staging_service.get_stage_order_by_id(staging_id)
        if not stage_order or not stage_order.status == StagingStatus.staging_successful:
            raise InvalidStatusException("Only deliver by staging_id if it has a successful status!"
                                         "Staging order was: {}".format(stage_order))

        delivery_order = self.delivery_repo.create_delivery_order(delivery_source=stage_order.get_staging_path(),
                                                                  delivery_project=delivery_project,
                                                                  delivery_status=DeliveryStatus.pending,
                                                                  staging_order_id=staging_id,
                                                                  md5sum_file=md5sum_file)

        args_for_run_mover = {'delivery_order_id': delivery_order.id,
                              'delivery_order_repo': self.delivery_repo,
                              'external_program_service': self.mover_external_program_service,
                              'session_factory': self.session_factory,
                              'path_to_mover': self.path_to_mover}

        if skip_mover:
            session = self.session_factory()
            delivery_order.delivery_status = DeliveryStatus.delivery_skipped
            session.commit()
        else:
            yield MoverDeliveryService._run_mover(**args_for_run_mover)

        return delivery_order.id

    @staticmethod
    def _parse_status_from_mover_info_result(mover_info_result):
        #Parse status from this type of example string:
        # Delivered: Jan 19 00:23:31 [1484781811UTC]
        pattern = re.compile('^(\w+):\s')
        hits = pattern.match(mover_info_result)
        if hits:
            return hits.group(1)
        else:
            raise CannotParseMoverOutputException("Could not parse mover info status from: {}".
                                                  format(mover_info_result))

    @gen.coroutine
    def _run_mover_info(self, mover_delivery_order_id):

        cmd = [os.path.join(self.path_to_mover, 'moverinfo'), '-i', mover_delivery_order_id]
        execution_result = yield self.moverinfo_external_program_service.run_and_wait(cmd)

        if execution_result.status_code == 0:
            mover_status = MoverDeliveryService._parse_status_from_mover_info_result(execution_result.stdout)
        else:
            raise CannotParseMoverOutputException("moverinfo returned a non-zero exit status: {}".
                                                  format(execution_result))
        return mover_status

    @gen.coroutine
    def update_delivery_status(self, delivery_order_id):
        """
        Check delivery status and update the delivery database accordingly
        """
        delivery_order = self.get_delivery_order_by_id(delivery_order_id)

        if delivery_order.mover_delivery_id and delivery_order.delivery_status == DeliveryStatus.delivery_in_progress:
            mover_info_result = yield self._run_mover_info(delivery_order.mover_delivery_id)
            session = self.session_factory()

            if mover_info_result == 'Delivered':
                log.info("Got successful status from Mover for delivery order: {}".format(delivery_order.id))
                delivery_order.delivery_status = DeliveryStatus.delivery_successful
            else:
                log.info("Got \"in progress\" status from Mover. Status was: {}".format(mover_info_result))

            session.commit()

        return delivery_order

    def get_delivery_order_by_id(self, delivery_order_id):
        return self.delivery_repo.get_delivery_order_by_id(delivery_order_id)

    def get_status_of_delivery_order(self, delivery_order_id):
        return self.get_delivery_order_by_id(delivery_order_id).delivery_status
