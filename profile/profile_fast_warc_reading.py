# Copyright (c) David Bern


import gzip


def main(filename):
    o = gzip.open if filename.endswith('.gz') else open

    with o(filename, 'rb') as f:
        offset = 0
        b = f.readline()
        c = 0
        content_length = None
        while b:
            if len(b) < 3:
                if content_length:
                    f.seek(content_length, 1)
                    content_length = 0
            else:
                if b != 'WARC/1.0\r\n':
                    name, value = b.split(':', 1)
                    value = value.strip()
                    if name.lower() == 'content-length':
                        content_length = int(value)
                else:
                    print offset
            c += 1
            offset = f.tell()
            b = f.readline()
        print "end:", f.tell()


def timeit_run(filename):
    import timeit
    filename = filename.replace('\\', '\\\\')
    setup = "from __main__ import main\r\nfilename='"+filename+"'"
    print(timeit.repeat("main(filename)",
                        setup=setup,
                        number=5,
                        repeat=3))

if __name__ == '__main__':
    import time
    start_time = time.time()
    file_name = 'D:\\Projects\\WARC\\wikipedia.warc.gz'
    #timeit_run(file_name)
    main(file_name)
    print("Elapsed time was %g seconds" % (time.time() - start_time))
