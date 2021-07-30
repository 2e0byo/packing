import struct
import math

try:
    import uos as os
except ImportError:
    import os


def pack_bools(bools):
    bool_byte = 0
    for i, x in enumerate(bools):
        bool_byte += int(x) << i
    return bool_byte


def unpack_bools(bool_byte):
    bools = []
    for bit in range(8):
        mask = 1 << bit
        bools.append((bool_byte & mask) == mask)
    return bools


class PackedReadings:
    def __init__(
        self, name, outdir, log_size, floats, bools=1, buffer_size=100, keep_logs=1
    ):
        self.bools = bools
        self.bool_bytes = math.ceil(bools / 8)
        self.floats = floats
        self.line_size = self.bool_bytes + len(struct.pack("f", 99.78)) * floats
        self.buffer_size = buffer_size
        self.log_size = log_size
        self.buf = bytearray(self.line_size * buffer_size)
        self.outdir = outdir
        self.keep_logs = keep_logs
        self.name = name
        self.pos = 0
        self.rotate_logs()

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

    @property
    def logf(self):
        return "{}/{}_0.bin".format(self.outdir, self.name)

    def rotate_logs(self):
        if self.keep_logs:
            logs = [
                int(fn.split("_")[-1].replace(".bin", ""))
                for fn in os.listdir(self.outdir)
                if fn.startswith(self.name)
            ]
            for i in (x for x in logs if x > self.keep_logs - 1):
                os.remove("{}/{}_{}.bin".format(self.outdir, self.name, i))
            for i in (x for x in logs if x <= self.keep_logs - 1):
                os.rename(
                    "{}/{}_{}.bin".format(self.outdir, self.name, i),
                    "{}/{}_{}.bin".format(self.outdir, self.name, i + 1),
                )

        else:
            os.remove(self.logf)

        self.pos = 0

    def write_log(self):
        with open(self.logf, "ab") as f:
            f.write(self.buf)

    def append(self, floats=None, bools=None):
        if self.pos % self.buffer_size == 0 and self.pos:
            self.write_log()
        pos = self.pos % self.buffer_size * self.line_size
        self.buf[pos : pos + self.line_size] = self.pack(floats, bools)

        if self.pos == self.log_size:
            self.rotate_logs()

        self.pos += 1

    def read(self, logf=None):
        if logf or self.pos > self.buffer_size:
            with open(logf if logf else self.logf, "rb") as f:
                while True:
                    seg = f.read(self.line_size)
                    if not seg:
                        break
                    yield self.unpack(seg)
        if not logf:
            for i in range(self.pos % self.buffer_size):
                pos = i * self.line_size
                yield self.unpack(self.buf[pos : pos + self.line_size])
