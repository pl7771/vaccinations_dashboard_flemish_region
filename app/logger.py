import logging
import logging.config
import traceback

from app import utils

from app.models.log import Log

def get_logger(name):
  try:
    logging.config.fileConfig("logger.ini")
  except Exception as e:
    print(e)  
  logger = logging.getLogger(name)
  return logger

class ETLLogHandler(logging.Handler):
  def emit(self, record):
    trace = None
    exc = record.__dict__['exc_info']
    if exc:
      trace = traceback.format_exc()
    with utils.db_session() as session:
      log = Log(
        logger=record.__dict__['name'],
        module=record.__dict__['module'],
        filename=record.__dict__['filename'],
        line=record.__dict__['lineno'],
        level=record.__dict__['levelname'],
        trace=trace,
        message=record.__dict__['msg'],
        created_at=record.__dict__['created'],
      )
      session.add(log)
      session.commit()
      session.close()
