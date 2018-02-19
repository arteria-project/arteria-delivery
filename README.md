Arteria delivery
=================

A self contained (Tornado) REST service that performs deliveries. Written for Python 3.6 or higher.

Trying it out
-------------
    
    # install dependencies
    pip install -r requirements/prod .
    

Try running it:

     delivery-ws --config config/ --port 8080 --debug

And then you can find a simple api documentation by going to:

    http://localhost:8080/api


REST endpoints
--------------

TODO

Making changes to the database model
--------------------------------------
Alembic is used to update the database, and migration scripts can be auto generated for most scenarios. However,
that means that when you need to make changes to the database models (i.e. all the models in
`delivery.models.db_models`) the you need to generate the revisions scripts. This can be done using the following
command:

    alembic -c config/alembic.ini revision --autogenerate -m '<your comment on what you changed>'

