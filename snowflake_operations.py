from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from snowflake.sqlalchemy import URL


class SnowflakeEngine():

    def __init__(self, user, pwd, account, warehouse):
        self.user = user
        self.pwd = pwd
        self.account = account
        self.warehouse = warehouse
        self.db = None
        self.schema = None
        self.table = None
        self.stage = None
        self.engine = None

    def set_db(self, db):
        self.db = db

    def set_schema(self, schema):
        self.schema = schema

    def set_table(self, table):
        self.table = table

    def set_stage(self, stage):
        self.stage = stage

    def make_engine(self, **kwargs):
        if self.db is None or self.schema is None:
            raise ValueError("Database and schema must be set before creating the engine")

        try:
            registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
            engine_url = self._construct_engine_url(**kwargs)
            engine = create_engine(engine_url)
            self.engine = engine
        except Exception as e:
            raise RuntimeError(f"Error occurred while creating the engine: {str(e)}")

    def _construct_engine_url(self, **kwargs):
        engine_url = URL(
            account=self.account,
            user=self.user,
            password=self.pwd,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            **kwargs
        )

        return str(engine_url)

    def execute_commands(self, commands):
        if self.engine is None:
            raise RuntimeError("Engine is not initialized")

        try:
            with self.engine.connect() as con:
                for command in commands:
                    con.execute(command)
        except Exception as e:
            raise RuntimeError(f"Error occurred while executing SQL commands: {str(e)}")
