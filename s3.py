import re
import boto
import gzip
import sqlite3
import cStringIO as StringIO

LOG_BUCKET = "logs.hortont.com"
S3_PREFIXES = ["backups.hortont.com",
               "files.hortona.com",
               "files.hortont.com",
               "files.whatmannerofburgeristhis.com"]
CF_PREFIXES = ["files.hortona.com-cloudfront",
               "files.hortont.com-cloudfront",
               "files.whatmannerofburgeristhis.com-cloudfront"]

conn = boto.connect_s3()
bucket = conn.get_bucket(LOG_BUCKET)

def get_logs_for_prefix(prefix):
    for key in bucket.get_all_keys():
        if key.key.startswith(prefix):
            gzip_content = StringIO.StringIO(key.get_contents_as_string())
            yield (key, gzip.GzipFile(fileobj=gzip_content).read())

def parse_w3c_log(log):
    headers = {}
    rows = []

    for line in log.splitlines():
        header_match = re.search(r"^#([^:]*):\s*(.*)$", line)

        if header_match:
            headers[header_match.group(1)] = header_match.group(2)
        else:
            rows.append(line.split("\t"))

    return (headers, rows)

def create_table_from_log(parsed_log):
    headers, rows = parsed_log

    if "Fields" not in headers:
        return

    fields = re.split("\s+", headers["Fields"])
    table = []

    for row in rows:
        table_row = {}

        for field, column in zip(fields, row):
            table_row[re.sub("[^a-zA-Z]*", "", field).lower()] = column

        table.append(table_row)

    return table

def populate_db_with_table(db, table):
    print table

def create_cloudfront_db():
    db = sqlite3.connect(':memory:')

    db.execute("create table logs (scstatus text, scbytes text, csuriquery text, csmethod text, csreferer text, cshost text, csuristem text, time text, date text, cip text, xedgelocation text, csuseragent text)")

    return db

def __main__():
    cloudfront_db = create_cloudfront_db()

    logs = get_logs_for_prefix("files.hortont.com-cloudfront")
    table = []

    for parsed_log in [parse_w3c_log(log) for key, log in logs]:
        table.extend(create_table_from_log(parsed_log))

    populate_db_with_table(cloudfront_db, table)

if __name__ == "__main__":
    __main__()