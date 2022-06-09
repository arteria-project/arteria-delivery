import os.path
import shutil
import logging
import re
import json
from tornado import gen

from delivery.models.db_models import StagingStatus, DeliveryStatus
from delivery.exceptions import ProjectNotFoundException, TooManyProjectsFound, InvalidStatusException, CannotParseDDSOutputException

log = logging.getLogger(__name__)


class DDSService(object):

    def __init__(
            self,
            external_program_service,
            staging_service,
            staging_dir,
            delivery_repo,
            dds_project_repo,
            session_factory,
            dds_conf):
        self.external_program_service = external_program_service
        self.mover_external_program_service = self.external_program_service
        self.staging_service = staging_service
        self.staging_dir = staging_dir
        self.delivery_repo = delivery_repo
        self.dds_project_repo = dds_project_repo
        self.session_factory = session_factory
        self.dds_conf = dds_conf

    @staticmethod
    def _parse_dds_project_id(dds_output):
        log.debug('DDS output was: {}'.format(dds_output))
        pattern = re.compile(r'Project created with id: (snpseq\d+)')
        hits = pattern.search(dds_output)
        if hits:
            return hits.group(1)
        else:
            raise CannotParseDDSOutputException(f"Could not parse DDS project ID from: {dds_output}")

    async def create_dds_project(
            self,
            project_name,
            project_metadata,
            token_path):
        """
        Create a new project in dds
        :param project_name: Project name from Clarity
        :param project_metadata: dictionnary containing pi email, project
        description, owner and researcher emails as well as whether the data is
        sensitive or not.
        :param token_path: path to DDS authentication token.
        :return: project id in dds
        """
        cmd = [
                'dds',
                '--token-path', token_path,
                '--log-file', self.dds_conf["log_path"],
                '--no-prompt',
                ]

        cmd += [
                'project', 'create',
                '--title', project_name,
                '--description', f"\"{project_metadata['description']}\"",
                '-pi',  project_metadata['pi']
                ]

        cmd += [
                args
                for owner in project_metadata.get('owners', [])
                for args in ['--owner', owner]
                ]

        cmd += [
                args
                for researcher in project_metadata.get('researchers', [])
                for args in ['--researcher', researcher]
                ]

        if project_metadata.get('non-sensitive', False):
            cmd += ['--non-sensitive']

        log.debug(f"Running dds with command: {' '.join(cmd)}")
        execution_result = await self.external_program_service.run_and_wait(cmd)

        if execution_result.status_code == 0:
            dds_project_id = DDSService._parse_dds_project_id(execution_result.stdout)
        else:
            error_msg = f"Failed to create project in DDS: {execution_result.stderr}. DDS returned status code: {execution_result.status_code}"
            log.error(error_msg)
            raise RuntimeError(error_msg)

        self.dds_project_repo.add_dds_project(
                project_name=project_name,
                dds_project_id=dds_project_id)

        return dds_project_id

    @staticmethod
    @gen.coroutine
    def _run_dds_put(
            delivery_order_id,
            delivery_order_repo,
            staging_dir,
            external_program_service,
            session_factory,
            token_path,
            dds_conf):
        session = session_factory()

        # This is a somewhat hacky work-around to the problem that objects created in one
        # thread, and thus associated with another session cannot be accessed by another
        # thread, therefore it is re-materialized in here...
        delivery_order = delivery_order_repo.get_delivery_order_by_id(delivery_order_id, session)
        try:
            cmd = [
                    'dds',
                    '--token-path', token_path,
                    '--log-file', dds_conf["log_path"],
                    '--no-prompt',
                    ]

            cmd += [
                    'data', 'put',
                    '--mount-dir', staging_dir,
                    '--source', delivery_order.delivery_source,
                    '--project', delivery_order.delivery_project,
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

    @gen.coroutine
    def deliver_by_staging_id(
            self,
            staging_id,
            delivery_project,
            md5sum_file,
            token_path,
            skip_mover=False):

        stage_order = self.staging_service.get_stage_order_by_id(staging_id)
        if not stage_order or not stage_order.status == StagingStatus.staging_successful:
            raise InvalidStatusException("Only deliver by staging_id if it has a successful status!"
                                         "Staging order was: {}".format(stage_order))

        delivery_order = self.delivery_repo.create_delivery_order(
                delivery_source=stage_order.get_staging_path(),
                delivery_project=delivery_project,
                delivery_status=DeliveryStatus.pending,
                staging_order_id=staging_id,
                md5sum_file=md5sum_file)

        args_for_run_dds_put = {
            'delivery_order_id': delivery_order.id,
            'delivery_order_repo': self.delivery_repo,
            'staging_dir': self.staging_dir,
            'external_program_service': self.mover_external_program_service,
            'session_factory': self.session_factory,
            'token_path': token_path,
            'dds_conf': self.dds_conf,
            }

        if skip_mover:
            session = self.session_factory()
            delivery_order.delivery_status = DeliveryStatus.delivery_skipped
            session.commit()
        else:
            yield DDSService._run_dds_put(**args_for_run_dds_put)

        log.info(f"Removing staged runfolder at {stage_order.staging_target}")
        shutil.rmtree(stage_order.staging_target)

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
