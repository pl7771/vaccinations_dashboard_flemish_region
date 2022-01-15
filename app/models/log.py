from datetime import datetime

from app.models.base import Base
from sqlalchemy.sql.schema import Column

from sqlalchemy.sql.sqltypes import DateTime, String, Integer

from app.utils import send_mail

class Log(Base):
    __tablename__ = 'etl_logs'
    _ID = Column(Integer, primary_key=True)
    module = Column(String)
    filename = Column(String)
    line = Column(Integer)
    level = Column(String)
    trace = Column(String)
    message = Column(String)
    created_at = Column(DateTime)

    def __init__(self,
      logger=None,
      module=None,
      filename=None,
      line=None,
      level=None,
      trace=None,
      message=None,
      created_at=None,
    ):
      self.logger = logger
      self.module = module
      self.filename = filename
      self.line = line
      self.level = level
      self.trace = trace
      self.message = message
      self.created_at = datetime.fromtimestamp(created_at)

    def __unicode__(self):
      return self.__repr__()

    def __repr__(self):
      # return "<Log: %s>" % (self.msg[:50])
      return "<Log: %s - %s>" % (self.created_at.strftime('%m/%d/%Y-%H:%M:%S'), self.message[:50])
