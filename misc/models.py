from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Table, Integer, String, Boolean

Base = declarative_base()

class MacApp(Base):
    """
    Record all previously seen mac apps. Later crawls all previously seen apps.
    """
    __tablename__ = 'mac_apps'

    appId   = Column(Integer, primary_key=True, nullable=False)
    store   = Column(String(2), primary_key=True, nullable=False)


class MasCrawl(Base):
    """
    Record information about each crawl process
    """
    __tablename__ = 'mas_crawls'

    id          = Column(Integer, primary_key=True, unique=True)
    store       = Column(String(2), nullable=False) # 'de' or 'us', ...
    outfile     = Column(String(255), nullable=False)
    
    def __repr__(self):
        return '<MasCrawl: store=%s outfile=%s>' % (self.store, self.outfile)


def initiate_data_definitions(engine):
    Base.metadata.create_all(engine)
