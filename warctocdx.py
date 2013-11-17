# Copyright (c) David Bern


import os.path
from dateutil import parser as date_parser

from warcreader import warcreader, warc_record_parser
from cdxreader import cdx_entry, field_order_tostring


class warc_to_cdx_wrp(warc_record_parser):
    def __init__(self, finisher):
        self.prev_cdx = None
        super(warc_to_cdx_wrp, self).__init__(finisher)

    def get_record_header(self, name):
        m = self.headers.getRawHeaders(name)
        return m[0] if m and m[0] else None

    def headers_to_cdx(self):
        rh = self.get_record_header  # short hand
        c = cdx_entry()

        c.compressed_arc_file_offset = self.offset
        c.file_name = self.warcname
        c.date = date_parser.parse(rh('warc-date')).strftime('%Y%m%d%H%M%S')

        warc_type = rh('warc-type')
        c.mime_type = 'warc/' + warc_type  # Sometimes changed below

        if warc_type == 'warcinfo':
            c.mime_type = 'warcinfo'
            # warc name is supposed to go b/w the '//' but that breaks the
            # space delimitations
            c.original_url = 'warcinfo://pylibwarc-0.0.1'
            c.massaged_url = c.original_url

        elif warc_type == 'response':
            c.original_url = rh('warc-target-uri')
            c.mime_type = rh('content-type')
            rc = lambda x: x.split(b':')[1] if x and b':' in x else x
            c.new_style_checksum = rc(rh('warc-payload-digest'))
            c.massaged_url = cdx_entry.massage_url(c.original_url)

        elif warc_type.lower() == 'request':
            c.original_url = rh('warc-target-uri')
            c.massaged_url = cdx_entry.massage_url(c.original_url)
        return c

    def allHeadersReceived(self):
        self.new_cdx(self.headers_to_cdx())
        warc_record_parser.allHeadersReceived(self)


class warc_to_cdx(warcreader):
    parser = warc_to_cdx_wrp

    def __init__(self, fileobj, field_order, warcname):
        self.cdx_fh = fileobj
        self.field_order = field_order
        self.warcname = warcname

        self.cdx_fh.write(field_order_tostring(field_order) + '\r\n')
        self.prev_cdx = None
        super(warc_to_cdx, self).__init__()

    def write_cdx(self, cdx):
        if self.return_entries is not None:
            self.return_entries.append(cdx)
        self.cdx_fh.write(cdx.tostring(self.field_order))
        self.cdx_fh.write('\r\n')

    def new_cdx(self, cdx):
        if self.prev_cdx:
            self.prev_cdx.compressed_record_size =\
                cdx.compressed_arc_file_offset -\
                self.prev_cdx.compressed_arc_file_offset
            self.write_cdx(self.prev_cdx)
        self.prev_cdx = cdx

    def finished(self):
        if self.prev_cdx is not None:
            self.prev_cdx.compressed_record_size = self.file_size -\
                self.prev_cdx.compressed_arc_file_offset
            self.write_cdx(self.prev_cdx)
            self.prev_cdx = None
        self.cdx_fh.close()
        super(warc_to_cdx, self).finished()

    def create_parser(self):
        super(warc_to_cdx, self).create_parser()
        self._parser.new_cdx = self.new_cdx
        self._parser.warcname = self.warcname

    def readFile(self, name, use_gz=False, return_entries=False):
        self.return_entries = [] if return_entries else None
        super(warc_to_cdx, self).readFile(name, use_gz)
        return self.return_entries


def main():
    import argparse
    import time
    parser = argparse.ArgumentParser(description='WARC to CDX')
    parser.add_argument('-c', '--cdx', default='out.cdx',
                        help='CDX file to output to')
    parser.add_argument('warc', default='out.warc.gz',
                        nargs='?', help='WARC file to load')
    args = parser.parse_args()

    print "Indexing", args.warc
    start_time = time.time()
    fileobj = open(args.cdx, 'wb')
    field_order = 'N b a m s k r M S V g'.split()
    wReader = warc_to_cdx(fileobj, field_order, os.path.basename(args.warc))
    wReader.makeConnection(None)
    wReader.readFile(args.warc)
    print "Finished in", (time.time() - start_time), "seconds"

if __name__ == '__main__':
    main()
