from .models import MacApp, MasCrawl, initiate_data_definitions
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from contextlib import contextmanager
from typing import List

def make_engine(config):
    if config.DATABASE['ENGINE'] == 'sqlite':
        return create_engine('sqlite:///' + config.DATABASE['DATABASE_FILE'])
    elif config.DATABASE['ENGINE'] == 'postgresql':
        return create_engine('{}://{}:{}@{}/{}'.format(
            config.DATABASE['ENGINE'],
            config.DATABASE['USER'],
            config.DATABASE['PASSWORD'],
            config.DATABASE['ADDRESS'],
            config.DATABASE['NAME']))

    assert(false and 'Unsupported configuration specified.')


class Database:
    def __init__(self, config):
        self.engine = make_engine(config)
        self.session_maker = sessionmaker(bind=self.engine)

        # Populate the database with the models
        initiate_data_definitions(self.engine)


    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.session_maker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


    def get_mac_apps(self, session, store) -> List[int]:
        """
        Return a list of all mac app ids currently known for the target store
        """
        mac_apps = session.query(MacApp)\
                          .filter(MacApp.store == store)\
                          .all()

        return [app.appId for app in mac_apps]


    # Add a single new app to the database
    def add_mac_app(self, session, store, appId: int) -> bool:
        return self.add_mac_apps(session, store, [appId])


    # Add multiple apps at the same time to the db
    def add_mac_apps(self, session, store, appIds: List[int]):
        existing_apps = self.get_mac_apps(session, store)
        new_apps = set(appIds) - set(existing_apps)

        for appId in new_apps:
            app_entry = MacApp(appId=appId, store=store)
            session.add(app_entry)


    def get_mas_crawls(self, session) -> List[MasCrawl]:
        return session.query(MasCrawl)\
                      .all()


    def add_mas_crawl(self, session, crawl: MasCrawl):
        session.add(crawl)
    
