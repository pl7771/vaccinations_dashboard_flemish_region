from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declared_attr, declarative_base

metadata = MetaData()


class Base_:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


Base = declarative_base(cls=Base_, metadata=metadata)
