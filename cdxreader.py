# Copyright (c) David Bern


# TODO: Test get_record_type and massage_url

import gzip

import twisted.protocols.basic
from twisted.web.client import _URI

CDX_MAP = {
    'N': 'massaged_url',
    'b': 'date',
    'a': 'original_url',
    'm': 'mime_type',
    's': 'response_code',
    'k': 'new_style_checksum',
    'r': 'redirect',
    'M': 'meta_tags',
    'S': 'compressed_record_size',
    'V': 'compressed_arc_file_offset',
    'g': 'file_name'
}


class cdx_entry(object):
    @staticmethod
    def massage_url(url):
        if url is None or not isinstance(url, basestring):
            return None
        p = _URI.fromBytes(url)
        n = p.netloc.split('.')
        n.reverse()
        p.scheme = p.netloc = None
        return (','.join(n) + ')' + p.toBytes()).lower()

    def __init__(self, fields=None):
        fields = fields or []  # default to []
        f = lambda n: fields[n] if n in fields else None

        def toint(i):
            try:
                return int(i)
            except:
                return None
        self.massaged_url = f('N')
        self.date = f('b')
        self.original_url = f('a')
        self.mime_type = f('m')
        self.response_code = f('s')
        self.new_style_checksum = f('k')
        self.redirect = f('r')
        self.meta_tags = f('M')
        self.compressed_record_size = toint(f('S'))
        self.compressed_arc_file_offset = toint(f('V'))
        self.file_name = f('g')
        if self.massaged_url is None:
            self.massaged_url = self.massage_url(self.original_url)

    def get_record_type(self):
        if self.mime_type == 'application/http;msgtype=response':
            return 'response'
        if self.mime_type in ['warc-info', 'warcinfo']:
            return 'warcinfo'
        if self.mime_type == 'warc/request':
            return 'request'
        if self.mime_type == 'warc/resource':
            return 'resource'
        print "Unknown mime_type:", self.mime_type
        return None

    def tostring(self, field_order=None):
        field_order = field_order or 'N b a m s k r M S V g'.split()
        t = lambda n: str(n) if n is not None else '-'  # Only coerce None->'-'
        f = lambda n: t(getattr(self, CDX_MAP[n])) if n in CDX_MAP else '-'
        return ' '.join([f(k) for k in field_order])


def field_order_tostring(field_order):
    return ' CDX ' + ' '.join(field_order)


class cdx_reader(twisted.protocols.basic.LineOnlyReceiver):
    def __init__(self):
        self.field_order = None
        self.cdx_entries = []
        """:type : list of cdx_entry"""

    def parse_file(self, filename, use_gz=False):
        """
        :rtype: list of cdx_entry
        """
        o = gzip.open if use_gz or filename.endswith('.gz') else open
        with o(filename, 'rb') as f:
            for l in f:
                self.lineReceived(l)
        return self.cdx_entries

    def lineReceived(self, line):
        line = line.strip()
        if self.field_order is None:
            self.field_order = self.parseFieldOrder(line)
        else:
            e = self.parseEntryLine(self.field_order, line)
            self.entryReceived(e)

    def entryReceived(self, e):
        self.cdx_entries.append(e)

    @staticmethod
    def parseFieldOrder(line):
        p = line.split()
        if p[0] != 'CDX':
            raise ValueError("CDX header does not start with 'CDX'")
        return p[1:]

    @staticmethod
    def parseEntryLine(field_order, line):
        s = line.split()
        # Extra fields are all merged into the last field
        lfo = len(field_order)
        if len(s) > lfo:
            s = s[:lfo-1] + [' '.join(s[lfo-1:])]
        return cdx_entry(dict(zip(field_order, s)))


def main():
    import argparse
    import time

    arg_parser = argparse.ArgumentParser(description="CDX Reader")
    arg_parser.add_argument('-f', '--file', default='out.cdx.gz',
                            nargs='?', help='CDX file to load')
    parsed_args = arg_parser.parse_args()

    start_time = time.time()
    cdxr = cdx_reader()
    entries = cdxr.parse_file(parsed_args.file)
    print "Parsed", len(entries), "CDX entries in",\
        (time.time() - start_time), "seconds"

if __name__ == '__main__':
    main()
