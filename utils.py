import argparse
import logging
import os
import sys
import uuid
from contextlib import closing

import yaml
from snowflake.connector import connect

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream = logging.StreamHandler()
logger.addHandler(stream)


DEFAULT_APP_CONFIG_FILE = "snowflake_config.yaml"


def valid_yaml(val):
    """Check and make sure this is a yaml file and it exists

    Args:
        val (str): filename of yaml

    Returns:
        Filename if exists

    Raises:
        ArgumentTypeError: If file isn't .yaml or doesn't exist

    """
    if val.endswith(".yaml") and os.path.isfile(val):
        return val
    else:
        raise argparse.ArgumentTypeError(f"{val} is not a valid yaml config")


def get_cmd_args():
    """Get command line args. Expects user, password, and subject
    """
    parser = argparse.ArgumentParser(description="Create a new Snowflake project")
    parser.add_argument(
        "-u", "--user", help="Username to connect to Azure",
    )
    parser.add_argument(
        "-p", "--password", help="User password",
    )
    parser.add_argument(
        "-s", "--subject", help="Subject Area for the database",
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Whether to print or execute SQL statements",
        action="store_true",
    )
    args = parser.parse_args()

    return vars(args)


def get_app_config(cmd_conf):
    """
    Reads the app config from a given yaml file, combined with cmd line args.
    Returns combined config dict

    :param cmd_conf: config params that came in from the command line
    :type cmd_conf: dict
    """
    app_config_file_path = cmd_conf.get("app_config_file") or DEFAULT_APP_CONFIG_FILE
    logger.info(f"Reading App Config from {app_config_file_path}")
    with open(app_config_file_path, "r") as conf_file:
        conf_from_file = yaml.safe_load(conf_file)
    # remove None valued keys
    cmd_conf_valid = {k: v for k, v in cmd_conf.items() if v is not None}
    for key in cmd_conf_valid.keys():
        if key in conf_from_file.keys():
            logger.info(f"Overwriting {key} with {cmd_conf[key]}")
        else:
            logger.info(f"Appending new key to config: {key}")
        conf_from_file[key] = cmd_conf[key]
    logger.info(f"Config values to be used: {conf_from_file}")
    return conf_from_file


def create_db_names(subject):
    """
    Takes config dict and adds project subject where applicable

    :param sf_config: snowflake project configuration settings
    :type sf_config: dict
    """
    # create database names
    project_config = {}
    project_config["primary_database"] = f"{subject}_PROD"
    project_config["extra_databases"] = [f"{subject}_DEV", f"{subject}_TEST"]
    project_config["admin_role"] = f"{subject}ENGINEER"
    project_config["admin_user"] = f"{subject}_ENGINEER"
    project_config["standard_roles"] = [
        f"{subject}ANALYST",
        f"{subject}ENTERPRISE",
        f"{subject}TABLEAU",
    ]
    project_config["all_databases"] = [x for x in project_config["extra_databases"]]
    project_config["all_databases"].append(project_config["primary_database"])
    project_config["all_roles"] = [x for x in project_config["standard_roles"]]
    project_config["all_roles"].append(project_config["admin_role"])
    project_config["all_users"] = [x for x in project_config["standard_users"]]
    project_config["all_users"].append(project_config["admin_user"])
    project_config["all_warehouses"] = [x for x in project_config["all_users"]]

    return project_config
