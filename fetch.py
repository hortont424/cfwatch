import re
import boto
import gzip
import sqlite3
import cStringIO as StringIO

from util import open_cloudfront_log_db

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
    schema_fields = retrieve_schema(db, "logs")

    cur = db.cursor()

    for row in table:
        arg_accum = []

        for key in schema_fields:
            if not key in row:
                arg_accum.append("-")
            else:
                arg_accum.append(row[key])

        cur.execute("INSERT INTO logs VALUES ({0})".format(("?," * len(schema_fields))[:-1]), arg_accum)

    db.commit()

def create_cloudfront_log_db():
    db = open_cloudfront_log_db()

    if not retrieve_schema(db, "logs"):
        db.execute("create table logs (scstatus text, scbytes text, csuriquery text, csmethod text, csreferer text, cshost text, csuristem text, time text, date text, cip text, xedgelocation text, csuseragent text)")

    return db

def retrieve_schema(db, table_name):
    cur = db.cursor()
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND tbl_name=?", (table_name,))
    rows = list(cur)

    if not rows:
        return

    schema_fields = [s.split()[0] for s in re.search(r"\((.*)\)", rows[0][0]).group(1).split(", ")]

    return schema_fields

def import_cloudfront_logs(cloudfront_db, prefix):
    logs = list(get_logs_for_prefix(prefix))
    table = []

    for key, parsed_log in [(key, parse_w3c_log(log)) for key, log in logs]:
        print "Importing", key.key
        table.extend(create_table_from_log(parsed_log))

    populate_db_with_table(cloudfront_db, table)

    for key, log in logs:
        print "Deleting", key.key
        key.delete()

def __main__():
    cloudfront_db = create_cloudfront_log_db()

    for prefix in CF_PREFIXES:
        import_cloudfront_logs(cloudfront_db, prefix)

if __name__ == "__main__":
    __main__()