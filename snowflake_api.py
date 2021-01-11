import logging

from snowflake.connector import connect

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream = logging.StreamHandler()
logger.addHandler(stream)


class SnowflakeApi(object):
    """
    Class for Snowflake API connections
    """

    def __init__(self, conf):
        self.user = conf["user"]
        self.password = conf["password"]
        self.azure_acct = conf["azure_account"]
        if conf["debug"] is True:
            logger.warning("debug==True. All queries will print rather than execute")
        self.debug = conf["debug"]

    def get_sf_conn(self):
        """Returns a connection object
        """
        try:
            logger.info(f"Connecting to {self.azure_acct} as {self.user}")
            conn = connect(
                user=self.user, password=self.password, account=self.azure_acct,
            )
        except Exception as e:
            logger.error("Cannot create connection to Snowflake!")
            logger.error(e)
            raise
        else:
            return conn

    def run_sql(self, sql):
        """
        Runs a command or a list of commands. Pass a list of sql to the
        sql parameter to execute sequentially

        :param sql: sql (if str), or sequence of (if list) statements to be executed
        :type sql: str or list of str
        """
        debug = self.debug  # inherit debug from class instantiation
        if isinstance(sql, str):
            sql = [sql]

        if debug is False:  # only try to connect if not debugging
            conn = self.get_sf_conn()
            cur = conn.cursor()
        for s in sql:
            try:
                if debug is True:
                    logger.info(s)
                else:
                    logger.info(f"Executing: {s}")
                    cur.execute(s)
            except Exception as e:
                logger.error("Error executing command!")
                logger.error(e)
