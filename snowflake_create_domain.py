from snowflake_api import SnowflakeApi
from query_api import QueryApi
from utils import (
    get_app_config,
    get_cmd_args,
)


def generate_account_queries(subject, base_roles, wh_kwargs):
    """Generates queries for creating roles and warehouses

    :param subject: domain name, will be part of all object names
    :type subject: str
    :param base_roles: base roles to create, names will be prepended with subject
    :type base_roles: list of str
    :param wh_kwargs: kwargs of parameters for warehouse creation
    :type wh_kwargs: dict
    """
    query = QueryApi()
    queries = ["USE ROLE SYSADMIN;"]
    for role in base_roles:
        role_name = f"{subject}{role}"
        warehouse_name = f"{subject}_{role}"
        queries.append(query.create_warehouse(warehouse_name, **wh_kwargs))
    queries.append("USE ROLE SECURITYADMIN;")
    for role in base_roles:
        role_name = f"{subject}{role}"
        warehouse_name = f"{subject}_{role}"
        queries.append(query.create_role(role_name))
        queries.append(
            query.grant_role(
                role_name=role_name, grantee_type="role", grantee="SYSADMIN"
            )
        )
        queries.append(
            query.grant_role(
                role_name=role_name, grantee_type="role", grantee="SECURITYADMIN"
            )
        )
        queries.append(
            query.grant_to_role(
                "warehouse", warehouse_name, role_name, ["operate", "usage"]
            )
        )
    return queries


def generate_database_queries(subject):
    """Generates queries for creating databases and schemas

    :param subject: domain name, will be part of all object names
    :type subject: str
    """
    query = QueryApi()
    queries = ["USE ROLE SYSADMIN;"]
    queries.append(query.create_database(f"{subject}_PROD"))
    queries.append(f"USE DATABASE {subject}_PROD;")
    queries.append(query.create_schema("PROJECT"))
    queries.append(query.create_schema("INTEGRATE"))
    queries.append(query.create_database(f"{subject}_DEV", f"{subject}_PROD"))
    queries.append(query.create_database(f"{subject}_QA", f"{subject}_PROD"))
    queries.append(query.create_database(f"{subject}_TEST", f"{subject}_PROD"))
    return queries


def generate_admin_grants(subject):
    """Generates grant queries for admin (ENGINEER) role

    :param subject: domain name, will be part of all object names
    :type subject: str
    """
    role_name = f"{subject}ENGINEER"
    databases = [
        f"{subject}_PROD",
        f"{subject}_DEV",
        f"{subject}_QA",
        f"{subject}_TEST",
    ]
    query = QueryApi()
    queries = ["USE ROLE SECURITYADMIN;"]
    for database in databases:
        queries.append(f"USE DATABASE {database};")
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="all privileges",
            )
        )
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants=["references", "select", "ownership"],
                on_future_type="views",
            )
        )
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="all privileges",
                on_future_type="tables",
            )
        )
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="all privileges",
                on_future_type="schemas",
            )
        )
    return queries


def generate_tableau_grants(subject):
    """Generates grant queries for Tableau Developer role

    :param subject: domain name, will be part of all object names
    :type subject: str
    """
    # grant all privileges on all three databases
    # grant references, select, ownership on future views in databases
    # grant all privileges on future tables in databases
    # grant all privileges on schemas
    role_name = f"{subject}TABLEAUDEV"
    databases = [
        f"{subject}_PROD",
        f"{subject}_DEV",
        f"{subject}_QA",
        f"{subject}_TEST",
    ]
    query = QueryApi()
    queries = ["USE ROLE SECURITYADMIN;"]
    for database in databases:
        queries.append(f"USE DATABASE {database};")
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="USAGE",
            )
        )
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="SELECT",
                on_future_type="views",
            )
        )
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="SELECT",
                on_future_type="tables",
            )
        )
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="USAGE",
                on_future_type="schemas",
            )
        )
    return queries


def generate_standard_grants(subject):
    """Generates SELECT grants for Enterprise and Analyst roles.
    Enterprise role can access only GLOBAL schema
    Analyst role can access any schema

    :param subject: domain name, will be part of all object names
    :type subject: str
    """
    roles = [f"{subject}ENTERPRISE", f"{subject}ANALYST"]
    database = f"{subject}_PROD"
    query = QueryApi()
    queries = ["USE ROLE SECURITYADMIN;"]
    queries.append(f"USE DATABASE {database};")
    for role_name in roles:
        queries.append(
            query.grant_to_role(
                object_type="database",
                object_name=database,
                role_name=role_name,
                grants="USAGE",
            )
        )
    queries.append(
        query.grant_to_role(
            object_type="database",
            object_name=database,
            role_name=f"{subject}ANALYST",
            grants="SELECT",
            on_future_type="views",
        )
    )
    queries.append(
        query.grant_to_role(
            object_type="database",
            object_name=database,
            role_name=f"{subject}ANALYST",
            grants="SELECT",
            on_future_type="tables",
        )
    )
    return queries


if __name__ == "__main__":
    cli_args = get_cmd_args()
    print(f" cli args: {cli_args}")
    sf_config = get_app_config(cli_args)
    base_roles = sf_config["roles"]
    sf = SnowflakeApi(sf_config)
    # sf.run_sql(generate_account_queries(sf_config["subject"], base_roles, sf_config))
    # sf.run_sql(generate_database_queries(sf_config["subject"]))
    # sf.run_sql(generate_admin_grants(sf_config["subject"]))
    sf.run_sql(generate_tableau_grants(sf_config["subject"]))
    # sf.run_sql(generate_standard_grants(sf_config["subject"]))
