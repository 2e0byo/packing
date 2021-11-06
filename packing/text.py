# TODO replace with proper upython test.
try:
    import uos as os

    nofileerror = OSError  # pragma: no cover
except ImportError:
    import os

    nofileerror = FileNotFoundError

import time
from collections import namedtuple

Line = namedtuple("Line", ("id", "timestamp", "line"))


class RotatingLog:
    def __init__(
        self,
        name,
        outdir,
        log_lines=100,
        keep_logs=1,
        timestamp=False,
        timestamp_interval=None,
        incorporate=True,
        ext="log",
    ):
        self.name = name
        self.outdir = outdir
        self.log_lines = log_lines
        self.keep_logs = keep_logs
        self.ext = ext
        self._to_read = 0
        self._read = 0
        self._offset = 0
        self.pos = 0
        self._abs_pos = 0
        self._line_size = 100  # chars in line
        self.timestamp = timestamp
        self.timestamp_interval = timestamp_interval
        if incorporate:
            self.incorporate_logs()
        else:
            self.rotate_logs()

        lines_to_full = self.max_lines - self.abs_pos
        if self.available(outdir) < self.fsize(lines_to_full):
            raise Exception("Insufficient space in outdir")

    @staticmethod
    def available(d):
        stat = os.statvfs(d)
        return stat[4] * stat[0]

    def fsize(self, lines):
        # assumes ascii
        # returns in bytes
        return self.line_size * lines

    @property
    def line_size(self):
        return self._line_size

    @property
    def abs_pos(self):
        return self._abs_pos + self.pos

    @property
    def max_lines(self):
        return self.log_lines * (1 + self.keep_logs)

    @property
    def read_pos(self):
        return self._offset - self._read

    def logf(self, n=0):
        return "{}/{}_{}.{}".format(self.outdir, self.name, n, self.ext)

    def writeln(self, line):
        line = "{}\n".format(line[: self.line_size])
        with open(self.logf(), "a") as f:
            f.write(line)

    def add_timestamp(self, line):
        if self.timestamp:
            return "{}#{}".format(round(time.time()), line)
        else:
            return line

    def append(self, line):
        line = self.add_timestamp(line)
        if self.pos == self.log_lines:
            self.rotate_logs()
        self.writeln(line)
        self.pos += 1

    def timestampify(self, line):
        if self.timestamp:
            try:
                timestamp = line.split("#")[0]
                line = "#".join(line.split("#")[1:])
                if not line:
                    return None, timestamp
                return line, time.localtime(int(timestamp))
            except ValueError:
                return "{}#{}".format(timestamp, line), None

        elif self.timestamp_interval:
            timestamp = time.time() - self.read_pos * self.timestamp_interval
            return line, time.localtime(timestamp)

        else:
            return (line, None)

    def _reader(self, logf, skip, pos=None):
        if pos is None:
            pos = self.abs_pos - self._offset
        try:
            with open(logf, "r") as f:
                for _ in range(skip):
                    f.readline()
                while self._read < self._to_read:
                    x = f.readline()
                    if not x:
                        break
                    line, timestamp = self.timestampify(x[:-1])
                    yield Line(pos + self._read, timestamp, line)
                    self._read += 1
        except nofileerror:
            pass

    def read(self, logf=None, n=None, skip=0):
        self._to_read = n if n else self.pos
        self._read = 0
        if logf:
            yield from self._reader(logf, skip, pos=0)
            return

        self._offset = skip + self._to_read
        if self._offset > self.abs_pos:
            self._to_read -= self._offset - self.abs_pos
            if self._to_read <= 0:
                return
            self._offset = skip + self._to_read

        fs, skip = divmod(self._offset - self.pos, self.log_lines)
        if fs >= 0:
            fs += 1
        skip = self.log_lines - skip
        fs = max(fs, 0)
        if fs:
            for i in range(fs, -1, -1):
                yield from self._reader(self.logf(i), skip)
                skip = 0
        else:
            skip = self.pos - self._offset
            yield from self._reader(self.logf(), skip)

    def logs_in_outdir(self):
        # warning: this is quite flaky
        # uPy has no glob
        logs = []
        for fn in os.listdir(self.outdir):
            if fn.startswith(self.name):
                logs.append(int(fn.split("_")[-1].replace(".{}".format(self.ext), "")))
        return logs

    def rotate_logs(self):
        if self.keep_logs:
            logs = self.logs_in_outdir()
            if 0 in logs:
                for i in (x for x in logs if x > self.keep_logs - 1):
                    os.remove(self.logf(i))
                for i in sorted(
                    (x for x in logs if x <= self.keep_logs - 1), reverse=True
                ):
                    os.rename(self.logf(i), self.logf(i + 1))

        else:
            try:
                os.remove(self.logf())
            except Exception:
                pass
        self._abs_pos += self.pos
        self.pos = 0

    def incorporate_logs(self):
        # incorporate anything else in the outdir
        logs = self.logs_in_outdir()
        if not logs:
            return

        count = 0

        for line in self.read(logf=self.logf(0), n=self.log_lines):
            count += 1
        if count < self.log_lines:
            self.pos += count
        else:
            self.rotate_logs()

        logs = self.logs_in_outdir()
        try:
            logs.remove(0)
        except ValueError:
            pass

        max_lines = self.max_lines
        for i in logs:
            flen = len([1 for l in self.read(logf=self.logf(i), n=self.log_lines)])
            if self.abs_pos + flen <= max_lines:
                self._abs_pos += flen
            else:
                os.unlink(self.logf(i))
