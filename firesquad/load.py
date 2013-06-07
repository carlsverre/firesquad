import multiprocessing, os, sys, itertools, csv, time, codecs, subprocess
from firesquad import database

class CSVDialect(csv.Dialect):
    delimiter = ","
    escapechar = "\\"
    lineterminator = "\r\n"
    quotechar = '"'

    doublequote = False
    quoting = csv.QUOTE_NONE
    skipinitialspace = False

class Load():
    def __init__(self, options):
        self.options = options
        csv.field_size_limit(sys.maxsize)
        self.aggregators = itertools.cycle(self.options.aggregators)
        self.work()

    def work(self):
        workers = []

        # make sure the output dir exists
        if self.options.finished_dir and not os.path.exists(self.options.finished_dir):
            os.mkdir(self.options.finished_dir)

        table_dirs = [os.path.join(self.options.tables_dir, d) for d in os.listdir(self.options.tables_dir)] if self.options.tables_dir else []
        table_dirs += [os.path.abspath(d) for d in self.options.table_dirs]

        for table_dir in table_dirs:
            table_name = os.path.basename(table_dir)
            files = [os.path.join(table_dir, f) for f in os.listdir(table_dir)]

            print "Processing table: %s" % table_name

            for f in files:
                while True:
                    workers = [worker for worker in workers if worker.is_alive()]
                    if len(workers) < self.options.workers:
                        break
                    else:
                        print "Running workers: %d" % len(workers)
                        time.sleep(0.5)

                print "Starting worker on %s with table %s" % (f, table_name)
                worker = Worker(table_name, f, self.aggregators.next(), self.options)
                worker.start()
                workers.append(worker)

            for worker in workers:
                worker.join()

            print "Finished processing table: %s" % table_name

class RowCounter():
    def __init__(self):
        self.count = 0
        self.col_count = 0

    def update(self, col_count):
        self.col_count = col_count
        self.increment()
        self.update = self.increment

    def increment(self, n=None):
        self.count += 1

    def generate_rows(self):
        row = "(" + ','.join(self.col_count * ["%s"]) + ")"
        return ','.join([row] * self.count)

def row_processor(rows, row_counter, prepend_null):
    for row in rows:
        n = 0
        if prepend_null:
            n += 1
            yield None
        for col in column_processor(row):
            n += 1
            yield col
        row_counter.update(n)

def column_processor(row):
    for col in row:
        if col == '\\N':
            yield "NULL"
        else:
            yield col

def lsof_file(path):
    proc = subprocess.Popen(['lsof', '-Pnf', '--', path], stdin=None, stdout=None, stderr=None)
    proc.wait()
    return proc.returncode == 0

class Worker(multiprocessing.Process):
    def __init__(self, table_name, csv_path, mysql_host, options):
        multiprocessing.Process.__init__(self)
        self.table_name = table_name
        self.csv_path = csv_path
        self.mysql_host = mysql_host
        self.options = options

    def run(self):
        mysql = database.connect(host=self.mysql_host, user="root", database=self.options.database)
        insert_prefix = "INSERT INTO %s VALUES " % self.table_name

        if lsof_file(self.csv_path):
            print "File is open by another process: %s" % (self.csv_path)
            return

        with open(self.csv_path, 'rb') as csv_file:
            # sniff file to figure out if it's comma or tab delimited
            dialect = CSVDialect()
            dialect.delimiter = self.options.csv_delimiter or self.get_delimiter(csv_file)

            reader = csv.reader(csv_file, dialect=dialect)
            while True:
                batch_iterator = itertools.islice(reader, self.options.rows_per_insert)
                row_counter = RowCounter()

                try:
                    args = [col for col in row_processor(batch_iterator, row_counter, self.options.prepend_null)]
                except csv.Error as e:
                    if "NULL byte" in str(e):
                        print "Found null byte in line, skipping."
                    else:
                        raise e

                if len(args) == 0:
                    break

                insert_stmt = insert_prefix + row_counter.generate_rows()
                try:
                    mysql.execute(insert_stmt, *args)
                except database.MySQLError as e:
                    print "mysql error on file %s:\n%s" % (self.csv_path, e)

        if self.options.finished_dir:
            # move the csv file to the output dir
            finished_table_dir = os.path.join(self.options.finished_dir, self.table_name)
            if not os.path.exists(finished_table_dir):
                try:
                    os.mkdir(finished_table_dir)
                except os.error:
                    pass
            os.rename(self.csv_path, os.path.join(finished_table_dir, os.path.basename(self.csv_path)))

    def get_delimiter(self, csv_file):
        b = 1024
        while True:
            try:
                sniff = csv.Sniffer().sniff(csv_file.read(b), "\t,")
                break
            except csv.Error:
                csv_file.seek(0)
                b *= 2
                if b > 10485760:    # only sniff up to 10MB
                    raise Exception("Could not determine delimiter for file %s" % self.csv_path)

        csv_file.seek(0)
        return sniff.delimiter
