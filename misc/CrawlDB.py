# Simple class that abstract the database layer
import psycopg2
# Needed to read the database config file.
import json
import sys
from typing import List
import os.path

DB_CONFIG_FILE = os.path.join(os.path.dirname(__file__), "database_config.json")

# The mac_apps table simply records each trackId for an application
# seen on the MAS. No further information is stored here -- the
# rest will likely be incorporated into a dedicated database
# The primary purpose of this table is to allow each crawl to
# crawl all previously seen apps, even if they were not found on
# the respective store page
MAC_APPS_SCHEMA = "CREATE TABLE mac_apps (trackId INTEGER NOT NULL)"

# The mac_crawls table records information about each
# crawl of a mac app store front.
# It should be noted that this schema is obviously not optimized.
# Nevertheless, due to the small rate of change, this data
# will not grow unreasonably large in the forseeable future
MAC_CRAWLS_SCHEMA = '''
CREATE TABLE mac_crawls (
			/* This serial is never set directly but instead
			automatically incremented by postgres */
			id SERIAL PRIMARY KEY,
			/* The store front.
			So far, either "de" or "us" */
			store char(2) NOT NULL,
			/* The output file as supplied to the scrapy spider.
			Currently, file:// might appear as a prefix. */
			outfile varchar(255) NOT NULL,
			/* Is the current crawl is in progress? */
			in_progress BOOLEAN,
			/* We have *daily* and *weekly* crawls.
			-> either "daily" or "weekly"
			daily crawls use the mac app store preview pages to find apps
			weekly crawls use previously found apps and try to find apps
			that are missing from the list */
			kind varchar(255) NOT NULL,
			/* Has the information of this crawl been incorporated 
			into the mac_apps table (see above)? */
			summarised BOOLEAN)
'''

# Logs an error to stderr and returns false,
# indicating that *something* did not execute
# successfully. False is returned to allow
# callers to simply return log_error(...)
def log_error(err : str) -> bool:
	print(err, file = sys.stderr)
	return False

# Read config for a database.
# The config format is simply json.
# Two keys are expected that need no further documentation:
# 	`database_name`
# 	`database_user`
def db_read_config(config_file : str = DB_CONFIG_FILE) -> dict:
	response = {}

	with open(config_file) as config:
		response = json.loads(config.read())
		# Verify the config contains keys we require
		if not ("database_user" in response and "database_name" in response):
			log_error("Database config file invalid.")
			response = {}

	return response

# Check if a table exists in the current database
def db_exists_table(connection : object, table_name : str) -> bool:
	c = connection.cursor()
	c.execute('SELECT EXISTS(SELECT relname FROM pg_class WHERE relname = %s AND relkind=\'r\')', (table_name, ));
	exists = c.fetchone()[0]
	c.close()
	return exists

# Create a table with the specified SQL. This is mainly just
# a wrapper around execute and does nothing special.
# It's main purpose is to provide "cleaner code"
def db_create_table(connection: object, table_sql: str) -> bool:
	if not table_sql.lower().startswith("create table"):
		return False

	c = connection.cursor()
	try:
		c.execute(table_sql)
		return True
	except:
		log_error("Failed to create table.")
		return False
	finally:
		c.close()

# This class models a simple crawl of the mac app store.
# The database entry that goes with this has the following
# declaration:
# (id INTEGER PRIMARY KEY,
#  store char(2),
#  outfile varchar(255),
#  in_progress bool,
#  kind varchar(255),
#  summarised bool)
# (See also above!)
class MacCrawl:
	# The (auto-generated) ID is the only data item that should have
	# no setter, because it may not be changed. Everything else is
	# explicitly set in the constructor
	__db_id = None

	# initialization_data should be a row fetched from the database
	# (including the id and everything else!)
	def __init__(self, initialization_data, db_connection = None, existing = True):
		# Make sure we are called with plausible data
		assert(type(initialization_data)) == tuple
		assert(len(initialization_data)) == 6
		# Checking the types of the individual data items
		assert(type(initialization_data[0])) == int
		assert(type(initialization_data[1])) == str
		assert(type(initialization_data[2])) == str
		assert(type(initialization_data[3])) == bool
		assert(type(initialization_data[4])) == str
		assert(type(initialization_data[5])) == bool

		self.__db_id = initialization_data[0]
		self.store = initialization_data[1]
		self.outfile = initialization_data[2]
		self.in_progress = initialization_data[3]
		self.kind = initialization_data[4]
		self.summarised = initialization_data[5]

		self.db_connection = db_connection

		# If this entry did not come from the database to begin with!
		if not existing:
			self.__db_id = None


	@classmethod
	def new_crawl(cls):
		default_data = (-1, "", "", False, "", False)
		return cls(default_data, existing = False)

	# The only pair of properties. These are needed
	# to make sure a client cannot change the id field.
	@property
	def id(self) -> int:
		return self.__db_id

	@id.setter
	def id(self, new_id: int) -> None:
		assert(False and "Changing the ID is prohibited.")

	def __str__(self):
		components = [self.id, self.store, self.outfile, self.in_progress, self.kind, self.summarised]
		str_components = [str(x) for x in components]
		return "(" + ", ".join(str_components) + ")"

	__repr__ = __str__

	# Persists the changes (or no changes) to the supplied database
	def persist(self, connection: object = None) -> bool:
		# Iff we set a connection object in the constructor, use this
		if self.db_connection:
			connection = self.db_connection

		# Make sure we have a connection object, otherwise abort
		assert(connection is not None)

		c = connection.cursor()
		try:
			if self.id == None:
				# Insert the data into the table and do not update existing data.
				c.execute('INSERT INTO mac_crawls (store, outfile, in_progress, kind, summarised) VALUES (%s, %s, %s, %s, %s)',
					(self.store, self.outfile, self.in_progress, self.kind, self.summarised))
			else:
				# Update existing data.
				c.execute('UPDATE mac_crawls SET store = %s, outfile = %s, in_progress = %s, kind = %s, summarised = %s WHERE id = %s',
					(self.store, self.outfile, self.in_progress, self.kind, self.summarised, self.id))
			connection.commit()
			return True
		except:
			return False
		finally:
			c.close()

class CrawlDB:
	connection = None
	cursor = None

	def __init__(self):
		# Construct parameters for db connection
		params = db_read_config()
		conn_str = "dbname=%s user=%s" % (params["database_name"], params["database_user"])
		try:
			self.connection = psycopg2.connect(conn_str)
			# The autocommit mode is useful because otherwise the database
			# frequently stops working altogether and needs an explicit rollback
			# issued. When using autocommit, this is not necessary.
			self.connection.autocommit = True
			self.cursor = self.connection.cursor()

			# Make sure the tables exist for when this object is used down the line
			self.setup_tables()
		except:
			log_error("Failed to open database. Make sure the database exists.")

	# The setup_tables method should only be used by the init function
	# It is responsible for creating the relevant tables, if they do not already
	# exist
	def setup_tables(self):
		assert(self.connection != None)
		assert(self.cursor != None)

		if not db_exists_table(self.connection, "mac_apps"):
			db_create_table(self.connection, MAC_APPS_SCHEMA)
		if not db_exists_table(self.connection, "mac_crawls"):
			db_create_table(self.connection, MAC_CRAWLS_SCHEMA)

	##
	### Functionality for the mac_apps table
	##
	def get_mac_apps(self) -> List[int]:
		self.cursor.execute('SELECT trackId FROM mac_apps')
		apps = [x[0] for x in self.cursor.fetchall()]
		return apps

	# Add a single new app to the database
	def add_mac_app(self, trackId : int) -> bool:
		return self.add_mac_apps([trackId])

	# Add multiple apps at the same time to the db
	def add_mac_apps(self, trackIds : List[int]) -> bool:
		# Make sure we are not adding apps we already have
		existing_apps = self.get_mac_apps()
		if set(trackIds) & set(existing_apps) != set():
			return log_error("Trying to add existing apps to db. Aborting")
		try:
			for trackId in trackIds:
				self.cursor.execute('INSERT INTO mac_apps (trackId) VALUES (%s)', (trackId, ))
			self.connection.commit()
		except:
			return log_error("Exception occurred while adding elements to database.")

		return True

	##
	### Functionality for the mac_apps table
	##
	def get_mac_crawls(self) -> List[MacCrawl]:
		self.cursor.execute('SELECT * FROM mac_crawls')
		results = self.cursor.fetchall()
		return [MacCrawl(result, self.connection) for result in results]

	def add_mac_crawl(self, crawl: MacCrawl) -> bool:
		return crawl.persist(self.connection)