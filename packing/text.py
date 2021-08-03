# TODO replace with proper upython test.
try:
    import uos as os

    nofileerror = OSError
except ImportError:
    import os

    nofileerror = FileNotFoundError


class RotatingLog:
    def __init__(self, name, outdir, log_lines=100, keep_logs=1):
        self.name = name
        self.outdir = outdir
        self.log_lines = log_lines
        self.keep_logs = keep_logs
        self.ext = "log"
        self._to_read = 0
        self.pos = 0
        self.maxlen = 100  # chars in line
        self.rotate_logs()

    def logf(self, n=0):
        return "{}/{}_{}.{}".format(self.outdir, self.name, n, self.ext)

    def writeln(self, line):
        line = "{}\n".format(line[: self.maxlen])
        with open(self.logf(), "a") as f:
            f.write(line)

    def append(self, line):
        if self.pos == self.log_lines:
            self.rotate_logs()
        self.writeln(line)
        self.pos += 1

    def _reader(self, logf, skip):
        try:
            with open(logf, "r") as f:
                for _ in range(skip):
                    f.readline()
                while self._to_read:
                    yield f.readline()[:-1]
                    self._to_read -= 1
        except nofileerror:
            pass

    def read(self, logf=None, n=None, skip=0):
        self._to_read = n if n else self.pos
        if logf:
            yield from self._reader(logf, skip)
            return

        total_lines = skip + self._to_read
        fs, skip = divmod(total_lines - self.pos, self.log_lines)
        fs = max(fs, 0)
        if fs:
            for i in range(fs, -1, -1):
                yield from self._reader(self.logf(i), skip)
                skip = 0
        else:
            skip = self.pos - total_lines
            yield from self._reader(self.logf(), skip)

    def rotate_logs(self):
        if self.keep_logs:
            logs = [
                int(fn.split("_")[-1].replace(".{}".format(self.ext), ""))
                for fn in os.listdir(self.outdir)
                if fn.startswith(self.name)
            ]
            for i in (x for x in logs if x > self.keep_logs - 1):
                os.remove(self.logf(i))
            for i in (x for x in logs if x <= self.keep_logs - 1):
                os.rename(self.logf(i), self.logf(i + 1))

        else:
            try:
                os.remove(self.logf())
            except Exception:
                pass
        self.pos = 0
