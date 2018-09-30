import scrapy
from scrapy.spidermiddlewares.httperror import HttpError

import re
import json

from misc.batch_list import BatchedList

def itunes_url_extract_id(url):
    # Common format:
    # https://itunes.apple.com/de/app/trello/id1278508951?mt=12
    match = re.match(r'^.*\/id(\d+)\?mt\=12$', url)
    if not match:
        return

    return match.group(1)


class MacAppSpider(scrapy.Spider):
    name = 'mac_apps'

    BATCH_SIZE = 100
    COUNTRY_CODE = None

    apps_queued = BatchedList(BATCH_SIZE)

    # Download at most one page every 3 seconds.
    # According to https://affiliate.itunes.apple.com/resources/documentation/itunes-enterprise-partner-feed/,
    # the feed cannot be accessed "as fast as possible"
    download_delay = 3

    start_urls = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MacAppSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal = scrapy.signals.spider_idle)
        return spider


    def __init__(self, *args, **kwargs):
        self.COUNTRY_CODE = kwargs.get('country_code', 'de')

        # 'known_apps' are the trackIds of previously crawled apps. Regardless
        # of whether they still are in the online inventory, we want to crawl them
        for app in kwargs.get('known_apps', []):
            self.apps_queued.add(str(app))

        self.start_urls.append('https://itunes.apple.com/' + self.COUNTRY_CODE + '/genre/mac/id39')


    def errback_custom(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)


    def queued_apps_add(self, url):
        self.apps_queued.add(itunes_url_extract_id(url))

    def queued_apps_get_request(self, batch=True):
        batch = self.apps_queued.getBatch(only_full=batch)
        if batch is None:
            return None

        query_string = ",".join(batch)

        return scrapy.Request(
            url = "https://itunes.apple.com/" + self.COUNTRY_CODE + "/lookup?id=" + query_string,
            callback = self.parse_api_response,
            errback = self.errback_custom)


    def spider_idle(self, spider):
        """
        Schedule any remaining requests.
        """
        if spider != self:
            return

        while True:
            api_request = self.queued_apps_get_request(batch=False)
            if api_request:
                self.logger.info("Crawling items from idle handler.")
                self.crawler.engine.crawl(api_request, self)
            else:
                break

        assert self.apps_queued.count() == 0


    def parse(self, response):
        # Extract links to genres
        for genre in response.css(".top-level-genre"):
            yield response.follow(genre, self.parse_genre)


    def parse_genre(self, response):
        # Extract links to alphabetical pages
        for alpha_href in response.css(".list").css(".alpha").css("a::attr(href)").extract():
            yield response.follow(alpha_href, self.parse_alphabetical_page)

            # Also process the website with the last letter as lowercase, as this site
            # also contains other apps!
            regex = r'^.*letter=([A-Z])$'
            match = re.match(regex, alpha_href)
            if match:
                lower_alpha_href = alpha_href[:-1] + alpha_href[-1].lower()
                yield response.follow(lower_alpha_href, self.parse_alphabetical_page)


    def parse_alphabetical_page(self, response):
        # Extract links to sub-sites, numbered from 1 to n
        sub_pages = response.css(".list").css(".paginate").css("a")
        if len(sub_pages) == 0:
            # Parse exactly two pages. The one we are on and the second one
            # this second page is not listed as existing, but often contains
            # an app or two.
            yield response.follow(response.url + '&page=1#page', self.parse_application_names)
            yield response.follow(response.url + '&page=2#page', self.parse_application_names)
        else:
            for sub_page in sub_pages:
                yield response.follow(sub_page, self.parse_application_names)

            # Parse one page after the end! 
            yield response.follow(response.url + ('&page=%d#page' % (len(sub_pages) + 1)), self.parse_application_names)


    def parse_application_names(self, response):
        for app_link in response.css("#selectedcontent").css("a::attr(href)").extract():
            self.queued_apps_add(app_link)

        request = self.queued_apps_get_request()
        if request:
            yield request


    def parse_api_response(self, response):
        results = json.loads(response.body_as_unicode())
        self.logger.info("iTunes API returned %d results." % int(results["resultCount"]))

        for result in results["results"]:
            yield result