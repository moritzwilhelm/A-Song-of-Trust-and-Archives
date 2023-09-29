# Assessing the Dependability of Web Archives for Reproducible Web Security Measurements

## Abstract

In recent years, the research community has recognized the growing significance of
artifact evaluation. Nonetheless, the ever-changing and unpredictable nature of the
Web continues to present an unresolved challenge for achieving reproducible web measurements.
This thesis explores the potential of public web archives, with a particular
focus on the Internet Archive, in addressing this persistent issue.

Our analysis involves a comprehensive evaluation of the reliability of data sourced
from the Internet Archive. We first conduct a longitudinal analysis spanning 7.5 years,
ranging from 2016 to the present, to assess the extent of historical data coverage
within the Internet Archive. While previous research has heavily relied on the Internet
Archive to conduct historical web measurements, this reliance has largely been rooted
in trust. To assess the validity of this trust, we evaluate the consistency of data stored
in the Internet Archive via two case studies. Specifically, we analyze the prevalence
of both syntactic and semantic differences in security header configurations, as well
as variations in third-party JavaScript dependencies among Internet Archive snapshots
that are in close temporal proximity. Finally, we explore the feasibility of leveraging
the Internet Archive to simulate live web security measurements, thereby addressing
the challenge of replicability in such studies.

Our findings affirm that the Internet Archive offers an extensive and densely populated
repository of archival snapshots, highlighting its dependability for web measurements.
However,we detect subtle pitfalls when conducting archive-based measurements and offer
effective strategies for mitigation, including the concept of snapshot neighborhoods.
Furthermore, we present a series of best practices tailored for future archive-based
web measurements. In conclusion, we determine that the Internet Archive provides a
reliable foundation for conducting reproducible web measurements.

# Step 0: Configuration and Installation

## Step 0.1: Configuration

Before crawling data, you have to set up a database and configure this project to use the database.\
You have to set the corresponding constants in [configs/database.py](src/configs/database.py):

```python
# DATABASE
DB_USER = '<DB_USER>'
DB_PWD = '<DB_PWD>'
DB_HOST = '<DB_HOST>'
DB_PORT = 1337
DB_NAME = '<DB_NAME>'

# STORAGE
STORAGE = Path('<PATH/TO/DATA/DIRECTORY/>')
```

Replace the `DB_` variables with the corresponding values of your database.
Further, configure a path where crawling responses should be stored by setting the `STORAGE` variable accordingly.

### (OPTIONAL) Socks Proxies Configuration

If you want to reduce the crawling duration of the Internet Archive experiments by distributing the requests over
multiple IPs, add available SOCKS proxies to the corresponding mapping in
[configs/crawling.py](src/configs/crawling.py).

```python
SOCKS_PROXIES = {
    # '<PORT>': '<REMOTE>',
}
```

## Step 0.2: Installation

To install the project, first set up a virtual environment via

```shell
python3 -m venv venv
```

After that, source the created `venv`:

```shell
source venv/bin/activate
```

Next, install the python requirements via:

```shell
pip install -r requirements.txt
```

Finally, install the project via:

```shell
pip install src/
```

# Step 1: Data Collection

## Step 1.1: Crawl the Internet Archive

### Step 1.1.1: Historical data

To simulate the experiments conducted in this thesis and crawl the IA, just run:

```shell
python3 src/data_collection/collect_archive_data.py
```

## Step 1.1.2: Randomly sampled historical data
To collect historical data of randomly sampled domains of the full Tranco Top 1M, first apply the following diff to [data_collection/collect_archive_data.py](src/data_collection/collect_archive_data.py)

```
@@ -13,7 +13,7 @@ from data_collection.crawling import setup, reset_failed_archive_crawls, partiti

 WORKERS = 8

-TABLE_NAME = 'HISTORICAL_DATA'
+TABLE_NAME = 'RANDOM_SAMPLING_HISTORICAL_DATA'


 class ArchiveJob(NamedTuple):
@@ -46,7 +46,7 @@ def worker(jobs: list[ArchiveJob], table_name: str = TABLE_NAME) -> None:
                 """, (tranco_id, domain, timestamp, url, error.to_json()))


-def prepare_jobs(tranco_file: Path = get_absolute_tranco_file_path(),
+def prepare_jobs(tranco_file: Path = get_absolute_tranco_file_path('tranco_random_sample.1337.csv'),
                  timestamps: list[datetime] = TIMESTAMPS,
                  proxies: dict[str, str] | None = None,
                  n: int = NUMBER_URLS) -> list[ArchiveJob]:
```

After that, just run 

```shell
python3 src/data_collection/collect_archive_data.py
```

again.

### Step 1.1.3: Neighborhoods

**IMPORTANT: FIRST REVERT THE CHANGE APPLIED IN STEP 1.1.2**

This previous step will only crawl a single snapshot per url and timestamp.
If you want to crawl neighborhoods instead, first run:

```shell
python3 src/data_collection/proxy_crawl_manager.py neighborhood_indexes
```

and after that

```shell
python3 src/data_collection/proxy_crawl_manager.py neighborhoods
```

In addition to the raw archival data of the neighborhoods, we also want to collect corresponding snapshot metadata, like
their contributors.
To do so, run:

```shell
python3 src/data_collection/proxy_crawl_manager.py archive_metadata
```

### Step 1.1.4: Daily archival snapshots

To conduct the experiment to analyze the stability of archival snapshots, first set the timestamp
at [data_collection/proxy_crawl_manager.py](src/data_collection/proxy_crawl_manager.py) in line 102 to the current date:

```python
START_TIMESTAMP = datetime(2023, 7, 16, 12, tzinfo=UTC)
```

Then start the corresponding daily crawls, either manually via

```shell
python3 src/data_collection/proxy_crawl_manager.py daily_archive
```

Or alternatively, add a cronjob that calls the utility shell script we provide
at [start_archive_data_collection.sh](start_archive_data_collection.sh)

## Step 1.2: Crawl live data

To directly crawl the front page of all domains listed in the Tranco file run:

```shell
python3 src/data_collection/collect_live_data.py
```

Instead, e.g., if you want to set up a cronjob, we also provide a utility shell script
at [start_live_data_collection.sh](start_live_data_collection.sh).

## Step 1.3: Crawl recent historical data used for comparison with live data

To collect recent archival data that can be used to compare against live data, first apply the following diff
to [data_collection/collect_archive_data.py](src/data_collection/collect_archive_data.py):

```
@@ -1,4 +1,4 @@
-from datetime import datetime
+from datetime import datetime, timedelta
 from multiprocessing import Pool
 from pathlib import Path
 from time import sleep
@@ -6,14 +6,14 @@ from typing import NamedTuple

 import requests

-from configs.crawling import NUMBER_URLS, INTERNET_ARCHIVE_URL, INTERNET_ARCHIVE_TIMESTAMP_FORMAT, TIMESTAMPS
+from configs.crawling import NUMBER_URLS, INTERNET_ARCHIVE_URL, INTERNET_ARCHIVE_TIMESTAMP_FORMAT, TIMESTAMPS, TODAY
 from configs.database import get_database_cursor
 from configs.utils import get_absolute_tranco_file_path, get_tranco_data
 from data_collection.crawling import setup, reset_failed_archive_crawls, partition_jobs, CrawlingException, crawl

 WORKERS = 8

-TABLE_NAME = 'HISTORICAL_DATA'
+TABLE_NAME = 'HISTORICAL_DATA_FOR_COMPARISON'

@@ -72,7 +72,7 @@ def main():
     setup(TABLE_NAME)

     # Prepare and execute the crawl jobs
-    jobs = prepare_jobs()
+    jobs = prepare_jobs(timestamps=[TODAY - timedelta(weeks=1)])
     run_jobs(jobs)
```

Then, analog to before, run:

```shell
python3 src/data_collection/collect_archive_data.py
```

# Step 2: Post-processing

Before running the analysis scripts, we have to apply some post-processing to the raw data to extract the included
JavaScript resources.
To do so, simply run:

```shell
python3 src/analysis/post_processing/extract_script_metadata.py
```

# Step 3: Analysis

Finally, we can run the analysis scritps.

## Step 3.1: Historical

First, we can analyze the coverage or archival snapshots:

```shell
python3 src/analysis/historical/analyze_coverage.py
```

Then, we can analyze the neighborhoods

```shell
python3 src/analysis/historical/analyze_neighborhoods.py
```

as well as their consistency regarding security header configurations

```shell
python3 src/analysis/historical/analyze_consistency.py
```

and third-party JavaScript dependencies

```shell
python3 src/analysis/historical/analyze_javascript.py
```

Finally, we can start the decision-tree-based analysis to attempt to attribute the inconsistencies within neighborhoods
to properties of their snapshots' metadata:

```shell
python3 src/analysis/historical/attribute_differences.py
```

## Step 3.2: Live

To analyze the stability of live data, run:

```shell
python3 src/analysis/live/analyze_stability.py
```

To compare live and archival data, run:

```shell
python3 src/analysis/live/analyze_disagreement.py
```

Finally, to compute the overhead of the Internet Archive, run:

```shell
python3 src/analysis/live/analyze_overhead.py
```

# Step 4: Plotting

Finally, we can plot the results of the analysis.
To do so, run all scripts in [plotting/historical_analysis/](src/plotting/historical_analysis/)
and [plotting/live_analysis/](src/plotting/live_analysis/) in arbitrary order.