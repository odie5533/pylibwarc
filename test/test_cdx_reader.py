# Copyright (c) David Bern


import unittest
from libwarc.cdxreader import cdx_reader, cdx_entry


class test_cdx_reader(unittest.TestCase):
    def test_parseFieldOrder(self):
        fo = ' CDX N b a m s k r M S V g'
        self.assertEqual(cdx_reader.parseFieldOrder(fo),
                         ['N', 'b', 'a', 'm', 's', 'k',
                          'r', 'M', 'S', 'V', 'g'])

    def test_parseFieldOrder_nocdx(self):
        fo = ' N b a m s k r M S V g'
        self.assertRaises(ValueError, cdx_reader.parseFieldOrder, fo)

    def test_parseFieldOrder_nostr(self):
        self.assertRaises(IndexError, cdx_reader.parseFieldOrder, '')
        self.assertRaises(AttributeError, cdx_reader.parseFieldOrder, None)

    def test_parser_good(self):
        fo = ' CDX N b a m s k r M S V g'
        o = '1 2 3 4 5 6 7 8 9 10 11'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s k r M S V g'.split())
        c.lineReceived(o)
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order), o)

    def test_parser_duplicate(self):
        fo = ' CDX N b a m s M r M V V N'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s M r M V V N'.split())
        c.lineReceived('1 2 3 4 5 6 7 8 9 10 11')
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order),
                         '11 2 3 4 5 8 7 8 10 10 11')

    def test_parser_short_field_order(self):
        """
        The extra fields should be merged into a single field
        """
        fo = ' CDX N b a m s g'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual('N b a m s g'.split(), c.field_order)
        c.lineReceived('1 2 3 4 5 6 7 8 9 10 11')
        self.assertEqual('6 7 8 9 10 11',
                         c.cdx_entries[0].file_name)
        self.assertEqual('1 2 3 4 5 6 7 8 9 10 11',
                         c.cdx_entries[0].tostring(c.field_order))

    def test_parser_long_field_order(self):
        fo = ' CDX N b a m s k r M S V g'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s k r M S V g'.split())
        c.lineReceived('1 2 3 4 5 6 7 8 9')
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order),
                         '1 2 3 4 5 6 7 8 9 - -')

    def test_parser_fake_fields(self):
        fo = ' CDX N b z m s q r X S V x'
        o = '1 2 3 4 5 6 7 8 9 10 11'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual('N b z m s q r X S V x'.split(), c.field_order)
        c.lineReceived(o)
        self.assertEqual(c.cdx_entries[0].tostring(c.field_order),
                         '1 2 - 4 5 - 7 - 9 10 -')

    def test_filename_space(self):
        fo = ' CDX N b a m s k r M S V g'
        o = '1 2 3 4 5 6 7 8 9 10 elev en.warc'
        c = cdx_reader()
        c.lineReceived(fo)
        self.assertEqual(c.field_order, 'N b a m s k r M S V g'.split())
        c.lineReceived(o)
        self.assertEqual('elev en.warc', c.cdx_entries[0].file_name)
        self.assertEqual(o, c.cdx_entries[0].tostring(c.field_order))

    def test_parser_sample(self):
        fo = ' CDX N b a m s k r M S V g'
        o = 'warcinfo:/wikipedia.warc.gz/archive-commons.0.0.1-SNAPSHOT-20120' \
            '2102659-python 20131109194250 warcinfo:/wikipedia.warc.gz/archiv' \
            'e-commons.0.0.1-SNAPSHOT-20120112102659-python warc-info - 2IGTQ' \
            'CWS2K2D3QYFZZZUCMIHHVSXMYGU - - 338 0 wiki pedia.warc.gz'
        c = cdx_reader()
        c.lineReceived(fo)
        c.lineReceived(o)
        e = c.cdx_entries[0]
        self.assertIsInstance(e, cdx_entry)
        self.assertEqual('warcinfo:/wikipedia.warc.gz/archive-c'
                         'ommons.0.0.1-SNAPSHOT-201202102659-python',
                         e.massaged_url)
        self.assertEqual('20131109194250', e.date)
        self.assertEqual('warcinfo:/wikipedia.warc.gz/archive-c'
                         'ommons.0.0.1-SNAPSHOT-20120112102659-python',
                         e.original_url)
        self.assertEqual('warc-info', e.mime_type)
        self.assertEqual('-', e.response_code)
        self.assertEqual('2IGTQCWS2K2D3QYFZZZUCMIHHVSXMYGU',
                         e.new_style_checksum)
        self.assertEqual('-', e.redirect)
        self.assertEqual('-', e.meta_tags)
        self.assertEqual(338, e.compressed_record_size)
        self.assertEqual(0, e.compressed_arc_file_offset)
        self.assertEqual('wiki pedia.warc.gz', e.file_name)
        self.assertEqual(o, e.tostring(c.field_order))

if __name__ == '__main__':
    unittest.main()
