# mas-crawl

The `mas-crawl` utility crawls iTunes / Mac App Store (MAS) metadata. As its initial data source, it uses the [Mac App Store Preview](https://itunes.apple.com/us/genre/mac/id39) pages that are accessible without spoofing the user agent. Subsequent scans query metadata for all apps previously found, in addition to parsing the preview website.

⚠️ This utility currently does not work as expected because the Mac App Store Preview website is non-functional as of now (April 9, 2020). To mitigate this issue, I've included a list of Mac App Store app IDs I have collected. See below for how to use this.

## Installation & Usage

```bash
# Clone the repo
$ git clone https://github.com/0xbf00/mas-crawl.git
# Install dependencies
$ cd mas-crawl
$ pip3 install -r requirements.txt
# Configure options
$ cp config.py.sample config.py
$ nano config.py
# Start crawling. Due to rate limiting employed by the iTunes API,
# crawls take a couple of hours (2-4)
python3 crawl.py --country_code us
```

Currently, the [Mac App Store Preview](https://itunes.apple.com/us/genre/mac/id39) website is non-functional. As a result, this utility will not work as expected. I have therefore included a copy of the app IDs I have collected for apps from the MAS. Note that this list is roughly a year old and will not include apps recently in the meantime. Here's how to make use of the file:

```bash
$ gunzip bootstrap.sql.gz
# Make sure to modify the database name below if you use a different name!
$ sqlite3 data.db < bootstrap.sql
```

## Design

The project uses [scrapy](https://scrapy.org/). The relevant crawler is implemented in `spider.py`, which is used by `crawl.py` to crawl directly, without using the `scrapy` command line utility.

Information about completed scans and app IDs of identified apps are stored in a database. This ensures consistent / somewhat usable results even when the Mac App Store Preview website changes significantly.

## Shortcomings

* The utility breaks when Apple decides to abandon / significantly change the Mac App Store Preview functionality. In this case, the tool will only crawl for information about previously found apps. This currently seems to be the case. This utility was used in 2018 and worked back then. Let me know if you know of a better way to /list apps/ on the MAS.
* When the utility works, crawls produce fairly big result files (100MB uncompressed, roughly 25MB when compressed). When running this utility continouosly, it would be beneficial to filter out results that haven't changed.
* iTunes rate limiting: The iTunes API is rate-limited and can only be called roughly every three seconds. The crawler will batch requests and asks for information on 100 apps at once. Still, crawling takes on the order of hours.