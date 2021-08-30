from .packed import PackedRotatingLog
from .text import nofileerror


class CachingPackedRotatingLog(PackedRotatingLog):
    def __init__(self, *args, buffer_size=100, **kwargs):
        super().__init__(*args, **kwargs)
        if buffer_size > self.log_lines:
            raise ValueError("Buffer should be smaller than file!")
        self.buffer_size = buffer_size
        self.buf = bytearray(self.line_size * buffer_size)
        self.pos = 0

    @property
    def buffer_pos(self):
        return self.pos % self.buffer_size

    def flush(self):
        for i in range(self.buffer_size):
            pos = i * self.line_size
            self.writeln(self.buf[pos : pos + self.line_size])

    def append(self, **kwargs):
        line = self.pack(**kwargs)

        if self.pos == self.log_lines:
            self.flush()
            self.rotate_logs()
        if self.buffer_pos == 0 and self.pos:
            self.flush()
        pos = self.buffer_pos * self.line_size
        self.buf[pos : pos + self.line_size] = line

        self.pos += 1

    def read(self, logf=None, n=None, skip=0):
        if not n:
            n = self.log_lines if logf else self.pos
        self._to_read = n
        self._read = 0

        if logf:
            skip = self.log_lines - n - skip
            yield from self._reader(logf, skip)
            return

        rows_in_f = self.pos - self.buffer_pos
        f_rows_needed = n + skip - self.buffer_pos

        if f_rows_needed > rows_in_f:
            # partial file is full file
            other_rows = f_rows_needed - rows_in_f
            fs, partial_f_rows = divmod(other_rows, self.log_lines)
            if skip:
                skip = self.log_lines - partial_f_rows
                fs += 1
        elif f_rows_needed > 0:
            fs = 0
            skip = rows_in_f - f_rows_needed
            partial_f_rows = f_rows_needed

        if f_rows_needed > 0:
            yield from self._reader(self.logf(fs), skip)
            skip = 0
            for i in range(fs - 1, -1, -1):
                yield from self._reader(self.logf(i), skip)
        else:
            skip = self.buffer_pos - n - skip

        # read from ram
        offset = self._read
        while self._read < self._to_read:
            i = self._read - offset
            pos = (skip + i) * self.line_size
            region = self.buf[pos : pos + self.line_size]
            if not len(region) or i == self.buffer_pos:
                break
            yield self.unpack(region)
            self._read += 1
