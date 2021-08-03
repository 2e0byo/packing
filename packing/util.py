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
