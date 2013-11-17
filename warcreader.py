# Copyright (c) David Bern


import gzip_offset_streamer as gzip
import twisted.internet.protocol
from twisted.web.http import _IdentityTransferDecoder
import twisted.web.http_headers
from twisted.web._newclient import HTTPParser


class warc_record_parser(HTTPParser, object):
    def __init__(self, finisher):
        self.finisher = finisher

    def headerReceived(self, name, value):
        self.headers.addRawHeader(name.lower(), value)

    def dataReceived(self, data):
        HTTPParser.dataReceived(self, data)

    def allHeadersReceived(self):
        clh = self.headers.getRawHeaders('content-length')

        if clh is None:
            content_length = None
        elif len(clh) == 1:
            content_length = int(clh[0])
        else:
            raise ValueError("Too many Content-Length headers;"
                             "WARC Record header is invalid")

        if content_length == 0 or content_length is None:
            transferDecoder = None
            self._finished(self.clearLineBuffer())
        else:
            transferDecoder = lambda x, y: _IdentityTransferDecoder(
                content_length, x, y)

        if transferDecoder is not None:
            if self.transport:
                self.transport.pauseProducing()
            td = transferDecoder(self.bodyDataReceived, self._finished)
            self.switchToBodyMode(td)

    def bodyDataReceived(self, data):
        pass

    def _finished(self, rest):
        self.state = 'DONE'
        self.finisher(rest)


class warcreader(twisted.internet.protocol.Protocol, object):
    parser = warc_record_parser

    def __init__(self):
        self._buffer = ''
        self.waiting_rec_clear = False
        self.gzip_offset = self.raw_offset = None
        self.file_size = None
        self.record_count = 0

    def create_parser(self):
        self._parser = self.parser(self.record_finished)
        self.set_offset(self.raw_offset)  # If using gzip, this is
        # overwritten after the first chunk of the member is read
        self._parser.connectionMade()

    def readFile(self, name, use_gz=False):
        self.raw_offset = 0
        self.create_parser()
        use_gz |= name.endswith('.gz')
        if use_gz:
            self.read_in_gzip(name)
        else:
            self.read_in_raw(name)
        self.finished()

    def read_in_raw(self, name):
        with open(name, 'rb') as f:
            f.seek(0, 2)
            self.file_size = f.tell()
            f.seek(0, 0)
            while True:
                b = f.read(4096)
                if not b:
                    break
                self.dataReceived(b)

    def set_offset(self, offset):
        self._parser.offset = offset

    def read_in_gzip(self, name):
        self.set_offset(0)
        self.offset = 0
        with gzip.GzipFile(name, 'rb') as f:
            f.fileobj.seek(0, 2)
            self.file_size = f.fileobj.tell()
            f.fileobj.seek(0, 0)
            while True:
                o, chunk = f.read_members()
                if chunk is None:
                    break
                self.set_offset(o)
                self.dataReceived(chunk)

    def send_parser_buffer(self):
        self.raw_offset += len(self._buffer)
        data = self._buffer
        self._buffer = ''
        self._parser.dataReceived(data)

    def dataReceived(self, d):
        self._buffer += d
        if self.waiting_rec_clear:
            if len(self._buffer) > 4:
                assert self._buffer[:4] == '\r\n\r\n'
                self.waiting_rec_clear = False
                self._buffer = self._buffer[4:]
                self.send_parser_buffer()
        else:
            self.send_parser_buffer()

    def _disconnectParser(self, reason):
        parser = self._parser
        self._parser = None
        parser.connectionLost(reason)

    def record_finished(self, rest):
        self.record_count += 1
        self.raw_offset = self.raw_offset - len(rest) + 4
        self._disconnectParser(None)

        self.create_parser()

        self.waiting_rec_clear = True
        self._buffer += rest
        self.dataReceived('')

    def finished(self):
        pass


def main():
    import argparse
    import time
    parser = argparse.ArgumentParser(description='WARC Reader')
    parser.add_argument('warc', default='out.warc.gz',
                        nargs='?', help='WARC file to load')
    args = parser.parse_args()

    print "Parsing file:", args.warc
    start_time = time.time()
    wReader = warcreader()
    wReader.makeConnection(None)
    wReader.readFile(args.warc)

    print "Parsed", wReader.record_count, "records in",\
        (time.time() - start_time), "seconds"

if __name__ == '__main__':
    main()
