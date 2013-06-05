import multiprocessing
import os
import subprocess
import itertools
import csv
import time

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
        self.aggregators = itertools.cycle(self.options.aggregators)
        self.work()

    def work(self):
        workers = []
        waiting_workers = []
        table_dirs = [os.path.join(self.options.tables_dir, t) for t in os.listdir(self.options.tables_dir)]
        for table_dir in table_dirs:
            table_name = os.path.basename(table_dir)
            files = [os.path.join(table_dir, f) for f in os.listdir(table_dir)]

            for f in files:
                while True:
                    waiting_workers = [worker for worker in (waiting_workers + workers) if worker.is_alive() and worker.waiting_on_memsql.is_set()]
                    workers = [worker for worker in workers if worker.is_alive() and not worker.waiting_on_memsql.is_set()]
                    if len(workers) < self.options.workers:
                        break
                    else:
                        time.sleep(0.5)

                print "Starting worker on %s with table %s" % (f, table_name)
                print "Stats: workers(%d) waiting_workers(%d)" % (len(workers), len(waiting_workers))
                worker = Worker(table_name, f, self.aggregators.next(), self.options.database)
                worker.start()
                workers.append(worker)

            for worker in waiting_workers:
                worker.join()
            for worker in workers:
                worker.join()

def column_generator(row):
    for col in row:
        if col == '\\N':
            yield "NULL"
        else:
            yield '"' + col.replace('"', '\\"') + '"'

def row_element_generator(rows):
    f1 = 0
    for row in rows:
        f2 = 0
        if f1:
            yield ","
        f1 = 1
        yield "("
        for col in column_generator(row):
            if f2:
                yield ","
            f2 = 1
            yield col
        yield ")"

class Worker(multiprocessing.Process):
    def __init__(self, table_name, csv_path, mysql_host, mysql_db, multiinsert_length=64, dialect="excel"):
        multiprocessing.Process.__init__(self)
        self.table_name = table_name
        self.csv_path = csv_path
        self.mysql_host = mysql_host
        self.mysql_db = mysql_db
        self.multiinsert_length = multiinsert_length
        self.dialect = dialect

        self.waiting_on_memsql = multiprocessing.Event()

    def run(self):
        mysql = subprocess.Popen(["mysql", "-u", "root", "-h", self.mysql_host, self.mysql_db], stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        self.insert_prefix = "INSERT INTO %s VALUES " % self.table_name

        with open(self.csv_path, 'rb') as csv_file:
            # sniff file to figure out if it's comma or tab delimited
            dialect = CSVDialect()
            try:
                sniff = csv.Sniffer().sniff(csv_file.read(102400), "\t,")
            except csv.Error:
                # could not determine delimiter!
                print "Could not determine delimiter for file %s" % self.csv_path
                os._exit(1)
            csv_file.seek(0)
            dialect.delimiter = sniff.delimiter

            reader = csv.reader(csv_file, dialect=dialect)
            n = 0
            while True:
                n += 1
                batch_iterator = list(itertools.islice(reader, self.multiinsert_length))
                if batch_iterator == []:
                    break
                self.insert(mysql.stdin, batch_iterator)

        # ensure mysql finishes
        self.waiting_on_memsql.set()
        stdoutdata, stderrdata = mysql.communicate()
        if mysql.returncode != 0:
            print "mysql error on file %s: \n%s\n\n%s" % (self.csv_path, stdoutdata, stderrdata)
        else:
            print "mysql finished with returncode %d" % mysql.returncode

    def insert(self, fd, rows):
        stmt = self.insert_prefix + ''.join([el for el in row_element_generator(rows)]) + ";"
        fd.write(stmt)
        fd.flush()
