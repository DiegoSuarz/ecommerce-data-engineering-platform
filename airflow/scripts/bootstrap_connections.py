import os

from airflow import settings
from airflow.models.connection import Connection


REQUIRED_ENV = (
    "MYSQL_DATABASE",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_CDC_USER",
    "MYSQL_CDC_PASSWORD",
    "POSTGRES_DATABASE",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
)


missing = [
    name
    for name in REQUIRED_ENV
    if not os.environ.get(name)
]

if missing:
    raise RuntimeError(
        "Airflow connection bootstrap missing "
        "required environment variables: "
        + ", ".join(missing)
    )


connection_specs = (
    {
        "conn_id": "mysql_source",
        "conn_type": "mysql",
        "host": "mysql",
        "port": 3306,
        "schema": os.environ["MYSQL_DATABASE"],
        "login": os.environ["MYSQL_USER"],
        "password": os.environ["MYSQL_PASSWORD"],
    },
    {
        "conn_id": "mysql_cdc_source",
        "conn_type": "mysql",
        "host": "mysql",
        "port": 3306,
        "schema": os.environ["MYSQL_DATABASE"],
        "login": os.environ["MYSQL_CDC_USER"],
        "password": os.environ["MYSQL_CDC_PASSWORD"],
    },
    {
        "conn_id": "postgres_dw",
        "conn_type": "postgres",
        "host": "postgres",
        "port": 5432,
        "schema": os.environ["POSTGRES_DATABASE"],
        "login": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    },
)


session = settings.Session()

try:
    for spec in connection_specs:
        connection = (
            session.query(Connection)
            .filter(
                Connection.conn_id
                == spec["conn_id"]
            )
            .one_or_none()
        )

        if connection is None:
            connection = Connection(
                conn_id=spec["conn_id"]
            )

            session.add(connection)
            action = "created"

        else:
            action = "updated"

        connection.conn_type = spec[
            "conn_type"
        ]
        connection.host = spec["host"]
        connection.port = spec["port"]
        connection.schema = spec["schema"]
        connection.login = spec["login"]
        connection.password = spec["password"]

        print(
            "Airflow connection "
            f"{action}: {spec['conn_id']}"
        )

    session.commit()

except Exception:
    session.rollback()
    raise

finally:
    session.close()


print(
    "Airflow connection bootstrap completed."
)
