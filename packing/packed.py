import struct
import math
from .util import pack_bools, unpack_bools
from .text import RotatingLog, nofileerror
from collections import namedtuple
import time

Line = namedtuple("line", ("id", "floats", "ints", "bools", "timestamp"))


class PackedRotatingLog(RotatingLog):
    def __init__(self, name, outdir, floats, ints, bools, **kwargs):
        self.floats = floats
        self.bools = bools
        self.ints = ints
        super().__init__(name, outdir, ext="bin", **kwargs)

    @property
    def bool_bytes(self):
        return math.ceil(self.bools / 8)

    @property
    def float_bytes(self):
        return len(struct.pack("f", 99.78) * self.floats)

    @property
    def timestamp_bytes(self):
        return len(struct.pack("l", round(time.time()))) if self.timestamp else 0

    @property
    def int_bytes(self):
        return len(struct.pack("i", 12346) * self.ints)

    @property
    def line_size(self):
        return (
            self.timestamp_bytes + self.bool_bytes + self.float_bytes + self.int_bytes
        )

    @property
    def struct_string(self):
        struct = "f" * self.floats + "i" * self.ints + "B" * self.bool_bytes
        if self.timestamp:
            return "l" + struct
        else:
            return struct

    def add_timestamp(self, line):
        # override as we do it in pack() and unpack()
        return line

    def pack(self, floats=None, ints=None, bools=None):
        if self.bool_bytes:
            bools = [
                pack_bools(bools[i : min(i + 8, len(bools))])
                for i in range(0, len(bools), 8)
            ]

        # micropython only allows one * expansion per line
        args = []
        if self.timestamp:
            args.append(round(time.time()))
        if floats:
            args += floats
        if ints:
            args += ints
        if bools:
            args += bools
        packed = struct.pack(self.struct_string, *args)
        return packed

    def timestampify(self, floats, ints, bools, timestamp):
        if timestamp:
            return floats, ints, bools, time.localtime(timestamp)
        elif self.timestamp_interval:
            timestamp = time.time() - self.read_pos * self.timestamp_interval
            return floats, ints, bools, time.localtime(timestamp)
        else:
            return floats, ints, bools, timestamp

    def unpack(self, packed):
        unpacked = struct.unpack(self.struct_string, packed)
        bools, ints, floats = (), (), ()
        timestamp = None
        if self.timestamp:
            timestamp = unpacked[0]
            unpacked = unpacked[1:]
        if self.bools:
            bools = []
            for byte in unpacked[-self.bool_bytes :]:
                bools += unpack_bools(byte)
            bools = tuple(bools)
        if self.ints:
            ints = unpacked[self.floats : self.floats + self.ints]
        if self.floats:
            floats = unpacked[: self.floats]
        return self.timestampify(floats, ints, bools, timestamp)

    def rotate_logs(self):
        super().rotate_logs()
        self.pos = 0

    def append(self, **kwargs):
        line = self.pack(**kwargs)
        super().append(line)

    def writeln(self, line):
        with open(self.logf(), "ab") as f:
            f.write(line)

    def _reader(self, logf, skip, pos=None):
        if pos is None:
            pos = self.abs_pos - self._offset

        try:
            with open(logf, "rb") as f:
                f.seek(skip * self.line_size)
                read_in_file = skip
                while self._read < self._to_read and read_in_file < self.log_lines:
                    seg = f.read(self.line_size)
                    if not seg:
                        break
                    yield Line(pos + self._read, *self.unpack(seg))
                    self._read += 1
                    read_in_file += 1
        except nofileerror:
            pass
