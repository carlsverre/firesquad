import subprocess
import os
import math
import itertools
import time
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BIN_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '../bin'))

class Split():
    def __init__(self, options):
        self.options = options
        cs = self.options.chunk_size
        if (cs & (cs - 1)) != 0:
            print "--chunk-size must be a power of 2!"
            sys.exit(1)
        self.split()

    def split(self):
        self.procs = {}
        for filename in self.options.files:
            output_dir = os.path.join(self.options.output_dir, os.path.basename(filename).replace('.csv', '')) + '/'
            try:
                os.makedirs(output_dir)
            except os.error:
                pass

            self.split_file(filename, output_dir)

        while True:
            self.kill_finished_procs()

            if len(self.procs) == 0:
                break

    def kill_finished_procs(self):
        for filename, proc in self.procs.items():
            if proc.poll() is not None:
                print "%s finished" % filename
                del(self.procs[filename])

    def split_file(self, filename, output_dir):
        prefix_gen = itertools.imap(lambda i: "%03d" % i, itertools.count(0))
        file_size = os.path.getsize(filename)
        read_all = False
        with open(filename, 'r') as fd:
            while not read_all:
                # ensure we arn't going to launch a dd process if we are at the parallelism limit
                while len([p for p in self.procs.values() if p.poll() is None]) >= self.options.parallelism:
                    self.kill_finished_procs()
                    time.sleep(.3)

                # store the current position
                b_start = fd.tell()
                # seek chunk_size bytes forward
                fd.seek(self.options.chunk_size, 1)
                # if we have passed the end of the file, lets seek back to the end
                if fd.tell() > file_size:
                    read_all = True
                    fd.seek(0, 2)

                # read until the next newline
                fd.readline()
                # get the end position
                b_end = fd.tell()

                # and then doooooo it
                output_filename = os.path.join(output_dir, prefix_gen.next())
                self.procs[output_filename] = self.dd_split(filename, b_start, b_end, output_filename)

    def dd_split(self, filename, b_start, b_end, output_filename):
        count, blocksize = self.factors(b_end - b_start)
        proc = subprocess.Popen([
            os.path.join(BIN_DIR, 'dd_bytes'),
            str(b_start),               # skip bytes before copy starts
            str(blocksize),             # each block is <bs> size in bytes
            str(count),                 # number of blocks to copy
            filename,                   # read from <if>
            output_filename             # write to <of>
        ], stdin=None, stdout=None, stderr=subprocess.PIPE)
        return proc

    def factors(self, n):
        if n % 2 == 0:
            # special case 2 so we can remove all even numbers from the search
            return 2, n / 2

        i = int(math.sqrt(n))
        for j in range(3, i + 1, 2):
            if n % j == 0:
                # at this point j is the smallest divisor so lets return the inverse
                return j, n / j

        # n is probably prime or some bullshit like that
        return 1, n
