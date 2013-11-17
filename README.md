pylibwarc
=========
pylibwarc is a Python library for dealing with Web ARChive (WARC) files. It has
a WARC reader, a CDX reader, and a warc to cdx converter. pylibwarc requires
the Twisted Python networking library as well as the Python dateutils library.

WARC to CDX
===========
To create a CDX index file from a WARC file, use:

    python warctocdx.py [-c <output.cdx.gz>] <warc file>
