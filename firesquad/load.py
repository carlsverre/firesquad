import multiprocessing
import os
import subprocess
import itertools
import csv
import time

class Load():
    def __init__(self, options):
        self.options = options
        self.work()

    def work(self):
        workers = []
        table_dirs = [os.path.join(self.options.tables_dir, t) for t in os.listdir(self.options.tables_dir)]
        for table_dir in table_dirs:
            table_name = os.path.basename(table_dir)
            files = [os.path.join(table_dir, f) for f in os.listdir(table_dir)]

            for f in files:
                while True:
                    workers = [worker for worker in workers if worker.is_alive()]
                    if len(workers) < self.options.workers:
                        break
                    else:
                        time.sleep(0.2)

                print "Starting worker on %s with table %s" % (f, table_name)
                worker = Worker(table_name, f, self.options.host, self.options.database)
                worker.start()
                workers.append(worker)

            for worker in workers:
                worker.join()

def column_generator(row):
    for col in row:
        if col == '\\N':
            yield "NULL"
        else:
            yield '"' + col + '"'

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
    def __init__(self, table_name, csv_path, mysql_host, mysql_db, multiinsert_length=50, dialect="excel"):
        multiprocessing.Process.__init__(self)
        self.table_name = table_name
        self.csv_path = csv_path
        self.mysql_host = mysql_host
        self.mysql_db = mysql_db
        self.multiinsert_length = multiinsert_length
        self.dialect = dialect

    def run(self):
        mysql = subprocess.Popen(["mysql", "-u", "root", "-h", self.mysql_host, self.mysql_db], stdin=subprocess.PIPE, stderr=None, stdout=None)
        self.insert_prefix = "INSERT INTO %s VALUES " % self.table_name

        with open(self.csv_path, 'rb') as csv_file:
            reader = csv.reader(csv_file, dialect=self.dialect)
            n = 0
            while True:
                n += 1
                batch_iterator = list(itertools.islice(reader, self.multiinsert_length))
                if batch_iterator == []:
                    break
                self.insert(mysql.stdin, batch_iterator)

        # ensure mysql finishes
        mysql.communicate()
        print "mysql finished with returncode %d" % mysql.returncode

    def insert(self, fd, rows):
        stmt = self.insert_prefix + ''.join([el for el in row_element_generator(rows)]) + ";"
        fd.write(stmt)
        fd.flush()
