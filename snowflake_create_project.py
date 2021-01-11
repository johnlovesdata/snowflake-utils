from snowflake_api import SnowflakeApi
from query_api import QueryApi
from utils import (
    get_app_config,
    get_cmd_args,
    create_db_names,
)


def generate_schema_queries(subject, project):
    """Generates queries for creating databases and schemas

    :param subject: domain name, will be part of all object names
    :type subject: str
    :param project: project to create as schema
    :type project: str
    """
    query = QueryApi()
    queries = ["USE ROLE SYSADMIN;"]
    databases = ["PROD", "TEST", "DEV"]
    for database in [f"{subject}_{x}" for x in databases]:
        queries.append(f"USE DATABASE {database};")
        queries.append(query.create_schema(project))
    return queries


if __name__ == "__main__":
    cli_args = get_cmd_args()
    sf_config = get_app_config(cli_args)
    print(generate_schema_queries(sf_config["subject"], "SCHEMA"))
