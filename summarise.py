'''Aggregate all results of daily scrapes
so that subsequent scrapes check at the minimum these
found apps
'''
import jsonlines
import psycopg2
import os.path
import gzip

from misc.CrawlDB import CrawlDB, MacCrawl

def main():
	db = CrawlDB()

	mac_crawls = db.get_mac_crawls()

	mac_apps = set()
	existing_apps = set( db.get_mac_apps() )

	for crawl in mac_crawls:
		# Skip if already processed
		if crawl.summarised:
			continue

		# Skip if currently in progress
		if crawl.in_progress:
			continue

		assert(crawl.outfile.startswith('file://') and "Expected output file to have file:// prefix.")
		filepath = crawl.outfile[len('file://'):]

		if os.path.exists(filepath):
			# Simply load the jsonlines file
			f = open(filepath)
		elif os.path.exists(filepath + ".gz"):
			# Load the compressed file into memory.
			f = gzip.open(filepath + '.gz')
		else:
			raise ValueError('Cannot load output file, because it does not exist.')

		# Record just the appId for later use.
		# All the other attributes can be queried using this identifier.
		with jsonlines.Reader(f) as reader:
			for item in reader:
				trackId = item["trackId"]
				if not trackId in existing_apps and trackId not in mac_apps:
					mac_apps.add(trackId)

		# Mark the entry as processed
		crawl.summarised = True
		crawl.persist()
		print("[+] Done processing crawl entry.")

	db.add_mac_apps(list(mac_apps))

	print("[+] Processing finished. Added %d new elements (%d already existing)" % (len(mac_apps), len(existing_apps)))

if __name__ == "__main__":
	main()