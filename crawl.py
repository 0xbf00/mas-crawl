"""
crawl.py: Programmatically invoke the MAS spider.
"""
from misc.db import Database
from misc.models import MacCrawl

import config

import scrapy.crawler

from spider import MacAppSpider

from datetime import datetime
from tempfile import mkstemp

import argparse
import os
import shutil
import jsonlines
import gzip

import http.client, urllib

def send_notification(msg):
    """
    Sends a pushover notification to all registered devices.
    """
    for device in config.PUSHOVER_DEVICE_TOKENS:
        conn = http.client.HTTPSConnection("api.pushover.net:443")

        conn.request("POST", "/1/messages.json",
          urllib.parse.urlencode({
            "token": config.PUSHOVER_APP_TOKEN,
            "user": device,
            "message": msg,
          }), { "Content-type": "application/x-www-form-urlencoded" })

        conn.getresponse()


def compress_file(outfile, keep_uncompressed=True):
    """
    Compresses the file contents of the outfile using the gzip algorithm.
    Creates a new file with the .gz extension in the same directory as the
    input file.
    """
    with open(outfile, 'rb') as f_in:
        with gzip.open(outfile + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    if not keep_uncompressed:
        os.remove(outfile)


def update_static_location(outfile, country_code):
    """
    Copy the result file to a predefined filesystem location.
    """
    static_path = os.path.join(
        config.LATEST_OUTPUT_BASEDIR,
        'mac_apps_{}_latest.jsonlines'.format(country_code))

    shutil.copy2(outfile, static_path)


def process_output_file(db, session, store, outfile):
    """
    Updates the mac_apps table, recording which apps have been
    seen previously.
    """
    assert(os.path.exists(outfile))

    n_existing = len(db.get_mac_apps(session, store))

    with jsonlines.open(outfile) as reader:
        appIds = [ item['trackId'] for item in reader if 'trackId' in item ]
        db.add_mac_apps(session, store, appIds)

    n_added = len(db.get_mac_apps(session, store)) - n_existing

    return (n_existing, n_added)


def get_output_filepath(country_code):
    output_file_components = [
        "mac_apps", 
        country_code,
        datetime.now().strftime("%Y-%m-%dT%H-%M-%S")]

    output_file = os.path.join(
        config.OUTPUT_BASEDIR, 
        "_".join(output_file_components) + "." + config.OUTPUT_FORMAT)

    assert(not os.path.exists(output_file) and "[err]: Outfile already exists.")

    return output_file


def run_spider(spider, settings, **kwargs):
    process = scrapy.crawler.CrawlerProcess(settings)
    process.crawl(spider, **kwargs)
    process.start()


def do_crawl(country_code):
    db = Database(config)

    output_file = get_output_filepath(country_code)

    with db.session_scope() as session:
        existing_apps = db.get_mac_apps(session, country_code)
        db.add_mac_crawl(session, MacCrawl(store=country_code, outfile=output_file))

    #1: Invoke the actual spider
    settings = {
        'FEED_URI': 'file://' + output_file,
        'LOG_LEVEL': 'WARNING'
    }
    run_spider(MacAppSpider, settings, known_apps=existing_apps, country_code=country_code)

    with db.session_scope() as session:
        #2: Process the result file. Update internal db.
        n_existing, n_added = process_output_file(db, session, country_code, output_file)

    #3: Update static locations (location where the latest metadata is available)
    update_static_location(output_file, country_code)

    #4: Compress the result file to save ~75% in required storage size
    compress_file(output_file, keep_uncompressed=False)

    #5: Notify registered devices that the crawl was successful.
    send_notification('"{}" store crawled with {} new entries.'.format(
        country_code, n_added))


def main():
    parser = argparse.ArgumentParser(description='Crawl the Mac App Store (MAS).')
    parser.add_argument('--country_code', default = 'de', 
        help = "The store id of the store to crawl.")
    args = parser.parse_args()

    do_crawl(args.country_code)


if __name__ == "__main__":
    main()