import multiprocessing, os, sys, itertools, csv, time, codecs
from firesquad import database

class CSVDialect(csv.Dialect):
    delimiter = ","
    escapechar = "\\"
    lineterminator = "\r\n"
    quotechar = '"'

    doublequote = False
    quoting = csv.QUOTE_NONE
    skipinitialspace = False

def unicode_csv_reader(unicode_csv_data, **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(unicode_csv_data, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, 'utf-8') for cell in row]

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
                worker = Worker(table_name, f, self.aggregators.next(), self.options.database, self.options.finished_dir)
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

def row_processor(rows, row_counter):
    for row in rows:
        n = 0
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

class Worker(multiprocessing.Process):
    def __init__(self, table_name, csv_path, mysql_host, mysql_db, finished_dir, multiinsert_length=64, dialect="excel"):
        multiprocessing.Process.__init__(self)
        self.table_name = table_name
        self.csv_path = csv_path
        self.mysql_host = mysql_host
        self.mysql_db = mysql_db
        self.multiinsert_length = multiinsert_length
        self.dialect = dialect
        self.finished_dir = finished_dir

        self.waiting_on_memsql = multiprocessing.Event()

    def run(self):
        mysql = database.connect(host=self.mysql_host, user="root", database=self.mysql_db)
        self.insert_prefix = "INSERT INTO %s VALUES " % self.table_name

        with open(self.csv_path, 'rb') as csv_file:
            # sniff file to figure out if it's comma or tab delimited
            dialect = CSVDialect()
            dialect.delimiter = self.get_delimiter(csv_file)

            reader = csv.reader(csv_file, dialect=dialect)
            while True:
                batch_iterator = itertools.islice(reader, self.multiinsert_length)
                row_counter = RowCounter()
                args = [col for col in row_processor(batch_iterator, row_counter)]

                if len(args) == 0:
                    break

                insert_stmt = self.insert_prefix + row_counter.generate_rows()
                try:
                    mysql.execute(insert_stmt, *args)
                except database.MySQLError as e:
                    print "mysql error on file %s:\n%s" % (self.csv_path, e)

        if self.finished_dir:
            # move the csv file to the output dir
            finished_table_dir = os.path.join(self.finished_dir, self.table_name)
            if not os.path.exists(finished_table_dir):
                os.mkdir(finished_table_dir)
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
