import jsonschema

# Status codes
OK = 200
ACCEPTED = 202
NO_CONTENT = 204

BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INTERNAL_SERVER_ERROR = 500


def exception_handler(postget_request):
    def wrapper(self, *args, **kwargs):
        try:
            postget_request(self, *args, **kwargs)
        except FileNotFoundError as file_not_found_error:
            self.set_status(NOT_FOUND)
            self.write(str(file_not_found_error))
        except PermissionError as permission_error:
            self.set_status(FORBIDDEN)
            self.write(str(permission_error))
        except RuntimeError as runtime_error:
            self.set_status(INTERNAL_SERVER_ERROR)
            self.write(str(runtime_error))
        except jsonschema.ValidationError as validation_error:
            self.set_status(BAD_REQUEST)
            self.write(str(validation_error))
    return wrapper
