import struct
import math

# TODO replace with proper upython test.
try:
    import uos as os

    nofileerror = OSError
except ImportError:
    import os

    nofileerror = FileNotFoundError

from .util import pack_bools, unpack_bools


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
        self.writeln(line)
        self.pos += 1
        if self.pos == self.log_size:
            self.rotate()

    def _reader(self, logf, skip):
        try:
            with open(logf, "r") as f:
                for _ in skip:
                    f.readline()
                while self._read_lines:
                    yield f.readline()
                    self._read_lines -= 1
        except nofileerror:
            pass

    def read(self, logf=None, n=None, skip=0):
        self._to_read = n
        if logf:
            yield from self._reader(logf, skip)
            return

        total_lines = n + skip
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
                int(fn.split("_")[-1].replace(self.ext, ""))
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


class PackedRotatingLog(RotatingLog):
    def __init__(self, name, outdir, floats, bools, **kwargs):
        self.floats = floats
        self.bools = bools
        self.ext = "bin"
        self.bool_bytes = math.ceil(bools / 8)
        self.line_size = self.bool_bytes + len(struct.pack("f", 99.78)) * floats
        super().__init__(name, outdir, **kwargs)

    def pack(self, floats=None, bools=None):
        if self.bool_bytes:
            bools = [
                pack_bools(bools[i : min(i + 8, len(bools))])
                for i in range(0, len(bools), 8)
            ]

        args = []
        if floats:
            args += floats
        if bools:
            args += bools
        packed = struct.pack("f" * self.floats + "B" * self.bool_bytes, *args)
        return packed

    def unpack(self, packed):
        unpacked = struct.unpack("f" * self.floats + "B" * self.bool_bytes, packed)
        if self.bools:
            floats = unpacked[: -self.bool_bytes]
            bools = []
            for byte in unpacked[-self.bool_bytes :]:
                bools += unpack_bools(byte)
            bools = bools[: self.bools]
        else:
            floats = unpacked
            bools = []
        return floats, bools

    def rotate_logs(self):
        super().rotate_logs()
        self.pos = 0

    def writeln(self, line):
        line = self.pack(floats, bools)
        with open(self.logf(), "ab") as f:
            f.write(line)

    def _reader(self, logf, skip):
        try:
            with open(logf, "rb") as f:
                f.seek(skip * self.line_size)
                while self._to_read:
                    seg = f.read(self.line_size)
                    if not seg:
                        break
                    yield self.unpack(seg)
                    self._to_read -= 1
        except nofileerror:
            pass

    class CachingPackedRotatingLog(PackedRotatingLog):
        def __init__(self, name, outdir, floats, bools, buffer_size=100, **kwargs):
            self.buffer_size = buffer_size
            self.buf = bytearray(self.line_size * buffer_size)
            super().__init__(name, outdir, floats, bools, **kwargs)

        def append(self, floats=None, bools=None):
            if self.buffer_pos == 0 and self.pos:
                self.writeln()
            pos = self.buffer_pos * self.line_size
            self.buf[pos : pos + self.line_size] = self.pack(floats, bools)

            if self.pos == self.log_size:
                self.rotate_logs()

            self.pos += 1

        def read(self, logf=None, n=None, skip=0):
            if not n:
                n = self.log_size if logf else self.pos
            self._to_read = n

            if logf:
                skip = self.log_size - n - skip
                yield from self._reader(logf, skip)
                return

            rows_in_f = self.pos - self.buffer_pos
            f_rows_needed = n + skip - self.buffer_pos

            if f_rows_needed > rows_in_f:
                # partial file is full file
                other_rows = f_rows_needed - rows_in_f
                fs, partial_f_rows = divmod(other_rows, self.log_size)
                if skip:
                    skip = self.log_size - partial_f_rows
                    fs += 1
            elif f_rows_needed:
                fs = 0
                skip = rows_in_f - f_rows_needed
                partial_f_rows = f_rows_needed

            if f_rows_needed:
                yield from self._reader(self.logf(fs), skip)
                skip = 0
                for i in range(fs - 1, -1, -1):
                    yield from self._reader(self.logf(i), skip)

            # read from ram
            i = 0
            while i < self._to_read:
                pos = (skip + i) * self.line_size
                region = self.buf[pos : pos + self.line_size]
                if not region or i == self.buffer_pos:
                    break
                yield self.unpack(region)
                i += 1
