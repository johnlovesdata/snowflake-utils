import logging
import uuid


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream = logging.StreamHandler()
logger.addHandler(stream)


class QueryApi(object):
    """
    Class for more easily generating administrative snowflake queries
    """

    def grant_to_role(
        self, object_type, object_name, role_name, grants, on_future_type=None
    ):
        """
        Grants one or more access privileges on a securable object to a role
        Privileges are object-specific and are grouped into the following categories:
            - Global privileges
            - Account objects (resource monitors, virtual warehouses, and databases)
            - Schemas
            - Schema objects (tables, views, stages, file formats, UDFs, and sequences)

        :param object_type: type to grant access (i.e. database, schema)
        :type object_type: str
        :param object_name: object to grant on
        :type object_name: str
        :param role_name: role to grant access
        :type role_name: str
        :param on_future_type: (optional) future objects to grant on, enum of {tables|views}
        :type on_future_type: str
        """
        if isinstance(grants, str):
            grants = [grants]
        if on_future_type:
            grant_template = f"FUTURE {on_future_type} IN {object_type} {object_name}"
        else:
            grant_template = f"{object_type} {object_name}"
        query = f"GRANT {','.join(grants)} ON {grant_template} TO ROLE {role_name};"
        return query

    def grant_role(self, role_name, grantee_type, grantee):
        """
        Assigns a role to a user or another role

        :param role_name: role to grant an account to
        :type role_name: str
        :param grantee_type: enum of {role|user}
        :type grantee_type: str
        :param grantee: specified account to grant role to
        :type grantee: str
        """
        query = f"GRANT ROLE {role_name} TO {grantee_type} {grantee};"
        return query

    def create_database(self, name, clone_database=None):
        """
        Creates a database

        :param name: database name
        :type name: str
        :param clone_database: (optional) database to clone from
        :type clone_database: str
        """
        clone_query = f" CLONE {clone_database}" if clone_database else ""
        query = f"CREATE DATABASE IF NOT EXISTS {name}{clone_query};"
        return query

    def create_schema(self, name):
        """
        Creates a schema

        :param name: schema name
        :type name: str
        """
        query = f"CREATE SCHEMA IF NOT EXISTS {name};"
        return query

    def create_warehouse(self, name, **kwargs):
        """
        Creates a warehouse owned by current user, with some parameters

        :param name: warehouse name
        :type name: str
        :param **kwargs: optional keyword arguments

        **kwargs
            warehouse_size (str): warehouse size, default to XLARGE
                valid values: XSMALL, SMALL, MEDIUM, LARGE, XLARGE, XXLARGE, XXXLARGE, X4LARGE
            auto_suspend (int): seconds of inactivity after which a warehouse is suspended, default 600
                valid values: any number greater than 59, or null
            auto_resume (bool): whether to auto resume when a query is submitted to it, default FALSE
            min_cluster_count (int): min number of server clusters, default 1
                valid values: 1 to 10, must be at most max cluster count
            max_cluster_count (int): max number of server clusters, default 1
                valid values: 1 to 10, must be at least min cluster count
            initially_suspended (bool): whether warehouse is created initially in the ‘Suspended’ state, default FALSE
        """
        if kwargs:
            valid_args = [
                "warehouse_size",
                "auto_suspend",
                "auto_resume",
                "min_cluster_count",
                "max_cluster_count",
                "initially_suspended",
            ]
            wh_with_options = []
            for key in kwargs.keys():
                if key in valid_args:
                    wh_with_options.append(f"{key} = {kwargs[key]}")
                else:
                    pass
            with_query = f" WITH {', '.join(wh_with_options)}"
        else:
            with_query = ""
        query = f"CREATE WAREHOUSE IF NOT EXISTS {name}{with_query};"
        return query

    def create_user(self, name, password=None, passwd_change=True):
        """
        Creates a user

        :param name: username
        :type name: str
        :param password: user password (optional)
        :type password: str
        :param passwd_change: Whether user needs to change password on first login
        :type passwd_change: bool
        """
        if not password:
            password = uuid.uuid1()
        user_options = f"PASSWORD = '{password}' MUST_CHANGE_PASSWORD = {passwd_change}"
        query = f"CREATE USER IF NOT EXISTS {name} {user_options};"
        return query

    def create_role(self, name):
        """
        Creates a role

        :param name: role name
        :type name: str
        """
        query = f"CREATE ROLE IF NOT EXISTS {name};"
        return query
