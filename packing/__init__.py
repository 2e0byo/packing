import struct
import math


def pack_bools(bools):
    bool_byte = 0
    for i, x in enumerate(bools):
        bool_byte += int(x) << i
    print(bin(bool_byte))
    return bool_byte


def unpack_bools(bool_byte):
    bools = []
    for bit in range(8):
        mask = 1 << bit
        bools.append((bool_byte & mask) == mask)
    return bools


class PackedReadings:
    def __init__(self, floats, bools=1):
        self.bools = bools
        self.bool_bytes = math.ceil(bools / 8)
        self.floats = floats

    def pack(self, floats=None, bools=None):
        if self.bool_bytes:
            bools = [
                pack_bools(bools[i : min(i + 8, len(bools))])
                for i in range(0, len(bools), 8)
            ]

        print(self.floats, self.bool_bytes, floats, bools)
        packed = struct.pack("f" * self.floats + "B" * self.bool_bytes, *floats, *bools)
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
