#!/bin/bash

echo "[+] Calling schedule.py with argument"
python3 "${BASH_SOURCE%/*}/schedule.py" --country_code $1
echo "[+] Calling summarise.py script."
python3 "${BASH_SOURCE%/*}/summarise.py"

# We really don't care about errors, which this thing throws
# if no files match the regex.
# gzip -9 /root/crawl_res/*.jsonlines