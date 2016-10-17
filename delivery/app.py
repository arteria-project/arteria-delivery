
from tornado.web import URLSpec as url

from arteria.web.app import AppService

from delivery.handlers.utility_handlers import VersionHandler

def routes(**kwargs):
    """
    Setup routes and feed them any kwargs passed, e.g.`routes(config=app_svc.config_svc)`
    Help will be automatically available at /api, and will be based on the
    doc strings of the get/post/put/delete methods
    :param: **kwargs will be passed when initializing the routes.
    """

    return [
        url(r"/api/1.0/version", VersionHandler, name="version", kwargs=kwargs)
    ]

def start():
    """
    Start the delivery-ws app
    """
    app_svc = AppService.create(__package__)
    app_svc.start(routes(config=app_svc.config_svc))