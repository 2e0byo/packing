import struct


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


def pack_status(temps, bools):
    bools = pack_bools(bools)
    packed = struct.pack("f" * len(temps) + "B", *temps, bools)
    return packed


def unpack_status(packed, no_temps):
    unpacked = struct.unpack("f" * no_temps + "B", packed)
    return unpacked[:-1], unpack_bools(unpacked[0])
