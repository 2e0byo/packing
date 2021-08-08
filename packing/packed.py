import struct
import math
from .util import pack_bools, unpack_bools
from .text import RotatingLog, nofileerror
from collections import namedtuple

Line = namedtuple("line", ("floats", "ints", "bools"))


class PackedRotatingLog(RotatingLog):
    def __init__(self, name, outdir, floats, ints, bools, **kwargs):
        self.floats = floats
        self.bools = bools
        self.ints = ints
        self.ext = "bin"
        super().__init__(name, outdir, **kwargs)

    @property
    def bool_bytes(self):
        return math.ceil(self.bools / 8)

    @property
    def float_bytes(self):
        return len(struct.pack("f", 99.78) * self.floats)

    @property
    def int_bytes(self):
        return len(struct.pack("i", 12346) * self.ints)

    @property
    def line_size(self):
        return self.bool_bytes + self.float_bytes + self.int_bytes

    @property
    def struct_string(self):
        return "f" * self.floats + "i" * self.ints + "B" * self.bool_bytes

    def pack(self, floats=None, ints=None, bools=None):
        if self.bool_bytes:
            bools = [
                pack_bools(bools[i : min(i + 8, len(bools))])
                for i in range(0, len(bools), 8)
            ]

            args = []

        # micropython only allows one * expansion per line
        if floats:
            args += floats
        if bools:
            args += bools
        if ints:
            args += ints
        packed = struct.pack(self.pack_string, *args)
        return packed

    def unpack(self, packed):
        unpacked = struct.unpack(self.pack_string, packed)
        bools, ints, floats = (), (), ()
        if self.bools:
            bools = []
            for byte in unpacked[-self.bool_bytes :]:
                bools += unpack_bools(byte)
            bools = tuple(bools)
        if self.ints:
            ints = unpacked[self.float_bytes : self.float_bytes + self.int_bytes]
        if self.floats:
            floats = unpacked[self.float_bytes]
        return Line(floats, ints, bools)

    def rotate_logs(self):
        super().rotate_logs()
        self.pos = 0

    def append(self, floats=None, bools=None):
        line = self.pack(floats, bools)
        super().append(line)

    def writeln(self, line):
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
