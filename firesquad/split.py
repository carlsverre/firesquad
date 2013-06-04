import subprocess
import os

class Split():
    def __init__(self, options):
        self.options = options
        self.split()

    def split(self):
        procs = {}
        for filename in self.options.files:
            target_prefix = os.path.join(self.options.output_dir, os.path.basename(filename).replace('.csv', '')) + '/'
            try:
                os.makedirs(target_prefix)
            except os.error:
                pass
            proc = subprocess.Popen(['split', '-l', str(self.options.chunk_size), filename, target_prefix])
            procs[filename] = proc
            print "Started processing %s into output dir %s" % (filename, target_prefix)

        while True:
            for filename, proc in procs.items():
                if proc.poll() is not None:
                    print "%s finished" % filename
                    del(procs[filename])

            if len(procs) == 0:
                break
