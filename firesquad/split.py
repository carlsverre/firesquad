import subprocess
import os
import math
import itertools
import time

class Split():
    def __init__(self, options):
        self.options = options
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
            for filename, proc in self.procs.items():
                if proc.poll() is not None:
                    print "%s finished" % filename
                    del(self.procs[filename])

            if len(self.procs) == 0:
                break

    def split_file(self, filename, output_dir):
        prefix_gen = itertools.imap(lambda i: "%03d" % i, itertools.count(0))
        file_size = os.path.getsize(filename)
        continue_working = True
        with open(filename, 'r') as fd:
            while continue_working:
                # store the current position
                b_start = fd.tell()
                # seek chunk_size bytes forward
                fd.seek(self.options.chunk_size, 1)
                # if we have passed the end of the file, lets seek back to the end
                if fd.tell() > file_size:
                    continue_working = False
                    fd.seek(0, 2)

                # read until the next newline
                fd.readline()
                # get the end position
                b_end = fd.tell()

                # and then doooooo it
                output_filename = os.path.join(output_dir, prefix_gen.next())
                self.procs[output_filename] = self.dd_split(filename, b_start, b_end, output_filename)

                while len([p for p in self.procs.values() if p.poll() is None]) > 16:
                    print "waiting for workers"
                    time.sleep(.1)

    def dd_split(self, filename, b_start, b_end, output_filename):
        count, blocksize = self.factors(b_end - b_start)
        proc = subprocess.Popen(['dd', 'count=%d' % count, 'bs=%d' % blocksize, 'if=%s' % filename, 'of=%s' % output_filename],
            stdin=None, stdout=None, stderr=subprocess.PIPE)
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

