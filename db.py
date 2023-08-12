import os
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy_utils import database_exists, create_database
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.sql import func
import pandas as pd
from sqlalchemy.schema import CreateSchema

import utils

# The base class which our objects will be defined on.
Base = declarative_base()


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]


class StaticDBConnection(metaclass=Singleton):
    """Make sure to close the connection once you finish."""

    def __init__(self, ssh: bool, ssh_user: str, ssh_host: str, ssh_pkey: str, delicate: str,
                 db_host: str, db_port: int, db_name: str, stream: bool, db_user: str, db_pass: str,
                 use_uri: bool, echo: bool = False, schema: str = None,
                 ):
        # SSH Tunnel Variables
        self.__engine = None
        self.__metadata = MetaData(schema=schema)
        self.echo = echo

        if ssh:
            self.__ssh_connect__(ssh_host, ssh_user, ssh_pkey, db_host, db_port)
        elif not ssh:
            self.local_port = db_port

        self.__create_database_engine__(
            delicate, stream, db_user, db_pass, db_host,
            self.local_port, db_name, use_uri
        )

    def __set_metadata(self, metadata):
        self.__metadata = metadata

    def __del_metadata(self):
        self.__metadata = None

    def __get_metadata(self):
        return self.__metadata

    metadata = property(
        fget=__get_metadata,
        fdel=__del_metadata,
        fset=__set_metadata,
    )

    def __set_engine(self, engine):
        self.__engine = engine

    def __del_engine(self):
        self.__engine = None

    def __get_engine(self):
        return self.__engine

    engine = property(
        fget=__get_engine,
        fdel=__del_engine,
        fset=__set_engine,
    )

    def __create_database_engine__(self, delicate, stream, db_user, db_pass, db_host, db_port, db, use_uri):
        utils.INFO(f"Creating connection to {db_host} on {db}...")
        if use_uri:
            conn_url = sqlalchemy.engine.URL.create(
                drivername=delicate,
                username=db_user,
                password=db_pass,
                host=db_host,
                database=db,
                port=db_port
            )
        else:
            conn_url = f'{delicate}://{db}'
        utils.INFO(f'Connection URI is: {conn_url}')
        self.__engine = create_engine(conn_url, echo=self.echo)
        if stream:
            self.__engine.connect().execution_options(stream_results=stream)
        utils.INFO(f'Database [{self.__engine.url.database}] session created...')

    def __ssh_connect__(self, ssh_host, ssh_user, ssh_pkey, db_host, db_port):
        utils.INFO("Establishing SSH connection ...")
        try:
            if os.path.isfile(ssh_pkey) or os.path.isdir(ssh_pkey):
                self.server = SSHTunnelForwarder(
                    ssh_host=(ssh_host, 22),
                    ssh_username=ssh_user,
                    ssh_private_key=ssh_pkey,
                    remote_bind_address=(db_host, db_port),
                )
            else:
                self.server = SSHTunnelForwarder(
                    ssh_host=(ssh_host, 22),
                    ssh_username=ssh_user,
                    ssh_password=ssh_pkey,
                    remote_bind_address=(db_host, db_port),
                )
            server = self.server
            server.start()  # start ssh server
        except Exception as e:
            utils.ERROR("Can't open a tunnel to the requested server. "
                        "please check if the server is reachable and/or the provided configurations is correct.")
            utils.ERROR(e)
            raise ConnectionError(
                "Can't open a tunnel to the requested server. "
                "please check if the server is reachable and/or the provided configurations is correct."
            )

        if db_host == 'locahost' or db_host == '127.0.0.1':
            self.local_port = server.local_bind_port
        else:
            self.local_port = db_port
        utils.INFO(f'Server connected via SSH || Local Port: {self.local_port}...')

    def schemas(self):
        inspector = sqlalchemy.inspect(self.engine)
        utils.INFO('Postgres database engine inspector created...')
        schemas = inspector.get_schema_names()
        schemas_df = pd.DataFrame(schemas, columns=['schema name'])
        utils.INFO(f"Number of schemas: {len(schemas_df)}")
        return schemas_df

    def tables(self, schema):
        inspector = sqlalchemy.inspect(self.engine)
        utils.INFO('Postgres database engine inspector created...')
        tables = inspector.get_table_names(schema=schema)
        tables_df = pd.DataFrame(tables, columns=['table name'])
        utils.INFO(f"Number of tables: {len(tables_df)}")
        return tables_df

    def select(self, query: str, chunksize=None):
        utils.INFO(f'Executing \n{query}\n in progress...')
        try:
            query_df = pd.read_sql(query, self.engine, chunksize=chunksize).convert_dtypes(convert_string=False)
        except Exception as e:
            utils.ERROR(f'Unable to read SQL query: {e}')
            raise e
        utils.INFO('<> Query Successful <>')
        return query_df

    def close(self):
        self.engine.dispose()
        utils.INFO('<> Connection Closed Successfully <>')


class Model:
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr[attr.rfind('__') + 2:], value

    def __str__(self):
        return f"{type(self).__name__}(\n" + ',\n'.join(
            [f"{attr[attr.rfind('__') + 1:]}={value}" for attr, value in self.__dict__.items()]
        ) + "\n)"

    def __repr__(self):
        return self.__str__()


class MetaModelBase(type(Base), type(Model)):
    pass


# Our Project object, mapped to the 'Projects' table
class Observation(Base, Model, metaclass=MetaModelBase):
    __tablename__ = 'image_observer'
    __table_args__ = {"schema": 'audit'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    photo_path = Column(String, nullable=False)
    predictions_path = Column(String)
    tbl_dt = Column(Integer, nullable=False)
    prediction_start_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, unique=False)
    prediction_end_time = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, unique=False)
    pid = Column(Integer, nullable=False)
    puser = Column(String, nullable=False)
    system = Column(String, nullable=False)
    node = Column(String, nullable=False)
    prediction_status = Column(String)
    event_type = Column(String, nullable=False)
    contain_faces = Column(Boolean)


def create_and_insert_observation(session, data: dict, commit=True):
    obs = Observation(**data)
    try:
        session.add(obs)
        if commit:
            session.commit()
    except Exception as e:
        session.rollback()
        utils.ERROR(f'Failed to insert observation  with id  {obs} into the database: {e}')
        raise e
    return obs


def create_observation(session, data: dict, ):
    obs = Observation(**data)
    try:
        session.add(obs)
    except Exception as e:
        session.rollback()
        utils.ERROR(f'Failed to insert observation  with id  {obs} into the database: {e}')
        raise e
    return obs


def commit_observations(session, ):
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        utils.ERROR(f"Failed to commit observations into the DB: {e}", )
        return False
    return True


def create_database_session(config: dict, ):
    try:
        connection = StaticDBConnection(
            # SSH SECTION
            ssh=config.get('USE_SSH'),
            ssh_user=config.get('SSH_USER'),
            ssh_host=config.get('SSH_HOST'),
            ssh_pkey=config.get('SSH_PRIVATE_KEY'),

            # DB SECTION
            delicate=config.get('DELICATE'),
            db_host=config.get('DB_HOST'),
            db_port=config.get('DB_PORT'),
            db_name=config.get('DB_NAME'),
            db_user=config.get('DB_USER'),
            db_pass=config.get('DB_PASSWORD'),
            stream=config.get('USE_STREAM'),
            use_uri=config.get('USE_URI'),
        )
        utils.INFO(connection)

        if not database_exists(connection.engine.url):
            create_database(connection.engine.url)

        conn = connection.engine.connect()
        if not conn.dialect.has_schema(conn, 'audit'):
            conn.execute(CreateSchema('audit'))
            conn.commit()

        if not conn.engine.dialect.has_table(conn, 'audit'):
            Base.metadata.create_all(connection.engine)
        else:
            utils.INFO("Database and Tables already exist. Establishing connection with DB construction.")

        conn.close()
        del conn

        # Creates a new session to the database by using the engine we described.
        return sessionmaker(bind=connection.engine)()
    except SQLAlchemyError as e:
        utils.ERROR(f"An error occurred while creating the database session: {e}")
        raise e
    except Exception as e:
        utils.ERROR(f"An unexpected error occurred while creating the database session: {e}")
        raise e


if __name__ == "__main__":
    # configs = env.Config('./config/local_config.ini')
    # dbref = create_database_session(config=configs)
    # print(dbref)
    pass
