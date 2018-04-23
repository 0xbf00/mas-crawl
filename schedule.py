"""
schedule.py:
This script is responsible for starting and monitoring of a scrapyd crawl of the
Mac App Store. It replaces a flaky shell script that resulted in functionality added to the
mac_scraper scrapy project that made this scraper very hard to maintain and prone to errors and bugs.
"""

from misc.CrawlDB import CrawlDB, MacCrawl

from scrapyd_api import ScrapydAPI

import argparse
import os
import time
import shutil

from datetime import datetime
from tempfile import mkstemp

OUTPUT_BASEDIR = "/root/crawl_res/"
OUTPUT_FORMAT = 'jsonlines'


def datestring_now():
    """Returns the current time in the format YYYY-mm-ddTHH-MM-SS"""
    return datetime.strftime(datetime.now(), "%Y-%m-%dT%H-%M-%S")


def create_output_file(country_code):
    """Create the output for a new crawl job and verify it does not exist yet."""
    output_file_components = ["mac_apps", country_code, datestring_now()]
    output_file = os.path.join(OUTPUT_BASEDIR, "_".join(output_file_components) + "." + OUTPUT_FORMAT)

    # Make sure the file does not exist yet.
    assert(not os.path.exists(output_file) and "[err]: Outfile already exists.")

    # Scrapy requires file name arguments that reference local files to be prepended with file://
    return "file://" + output_file


def scrapyd_schedule_mac_scraper(scrapyd_instance, db_instance, country_code, outfile, app_index) -> str:
    """Schedule a new scrapy job, returning the JOBID of the new job."""
    settings = {
        "FEED_URI": outfile
    }

    # Create a new entry in the datbase
    mac_crawl = MacCrawl.new_crawl()
    mac_crawl.store = country_code
    mac_crawl.outfile = outfile
    mac_crawl.in_progress = True
    mac_crawl.kind = 'daily'
    db_instance.add_mac_crawl(mac_crawl)

    return scrapyd_instance.schedule('mac_scraper', 'mac_apps',
        settings = settings, country_code = country_code, app_index = app_index)


def scrapyd_mac_scrape_finished(scrapyd_instance, jobid) -> bool:
    """Check whether a scrapyd job has finished."""
    current_status = scrapyd_instance.job_status('mac_scraper', jobid)
    # Make sure we get a valid response
    assert(current_status != "")
    return current_status == 'finished'


def crawl_for_outfile(db_instance, outfile):
    """
    Returns the crawl entry for a certain output file.
    :param db_instance: Database connection
    :param outfile: Filepath to output file
    :return: CrawlEntry
    :raises ValueError, if no CrawlEntry for a particular file was found.
    """
    crawls = db_instance.get_mac_crawls()
    for crawl in crawls:
        if crawl.outfile == outfile:
            return crawl
    else:
        raise ValueError("No crawl found for specified outfile.")


def mac_apps_index_file(db) -> str:
    '''
    Create a file in a temporary directory containing all apps previously
    encountered. Used by scraper component.
    :param db: Database connection
    :return: The filename of the temporary file
    '''
    apps = db.get_mac_apps()

    handle, temp_name = mkstemp(prefix='mac_apps_')
    os.close(handle)

    with open(temp_name, "w") as outfile:
        for app in apps:
            outfile.write("%d \n" % (app))

    return temp_name


def main():
    parser = argparse.ArgumentParser(description='Crawl the Mac App Store (MAS).')
    parser.add_argument('--country_code', default = 'de', help = "The store id of the store to crawl.")
    args = parser.parse_args()

    scrapyd = ScrapydAPI()
    crawldb = CrawlDB()

    outfile = create_output_file(args.country_code)
    # File containing all previously crawled apps
    app_index = mac_apps_index_file(crawldb)

    # Schedule crawl
    jobid = scrapyd_schedule_mac_scraper(scrapyd, crawldb, args.country_code, outfile, app_index)
    print("[+] Scrape scheduled. JOBID: %s" % (jobid))

    # White for job to finish
    while True:
        time.sleep(5)
        if scrapyd_mac_scrape_finished(scrapyd, jobid):
            break

    print("[+] Scrape finished.")

    # Delete temporary app index file
    os.remove(app_index)

    # Move latest file to fixed location to allow scripts to fetch this file using
    # scp
    shutil.copy2(outfile[len("file://"):],
                 "/root/mac_apps_{}_latest.jsonlines".format(args.country_code))
    print("[+] Static location updated with newest dump file.")

    # Update the database entry
    db_entry = crawl_for_outfile(crawldb, outfile)
    db_entry.in_progress = False
    db_entry.persist()

    print("[+] Updated database entry.")


if __name__ == "__main__":
    main()