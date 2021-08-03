import struct
import math
from .util import pack_bools, unpack_bools


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
