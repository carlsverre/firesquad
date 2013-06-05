import subprocess
import os
import math
import itertools
import time
import sys

class Split():
    def __init__(self, options):
        self.options = options
        if self.options.chunk_size % 2 != 0:
            print "--chunk-size must be an even number of bytes!"
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
                self.procs[output_filename] = self.dd_split(filename, b_start, b_end, output_filename, read_all)

                while len([p for p in self.procs.values() if p.poll() is None]) >= self.options.parallelism:
                    self.kill_finished_procs()
                    time.sleep(.3)

    def dd_split(self, filename, b_start, b_end, output_filename, read_all):
        if read_all:
            skip_count = b_start
            blocksize = 1
            count = b_end - b_start
        else:
            blocksize = self.options.chunk_size
            skip_count = b_start / blocksize
            count = 1
        proc = subprocess.Popen([
            'dd',
            'skip=%d' % skip_count,     # skip <skip_count> blocks
            'count=%d' % count,         # read 1 block
            'bs=%d' % blocksize,        # each block is <bs> size in bytes
            'if=%s' % filename,         # read from <if>
            'of=%s' % output_filename   # write to <of>
        ], stdin=None, stdout=None, stderr=subprocess.PIPE)
        return proc
