# Copyright (c) David Bern


import gzip


def main(filename):
    o = gzip.open if filename.endswith('.gz') else open

    with o(filename, 'rb') as f:
        while f.read(15*1024) != "":  # 1 MB
            pass


def timeit_run(filename, n, r):
    import timeit
    filename = filename.replace('\\', '\\\\')
    setup = "from __main__ import main\r\nfilename='"+filename+"'"
    m = timeit.repeat("main(filename)",
                      setup=setup,
                      number=n,
                      repeat=r)
    print m
    print min(m)


if __name__ == '__main__':
    import time, sys
    start_time = time.time()
    #timeit_run(sys.argv[1], 1, 3)
    main(sys.argv[1])
    print("Elapsed time was %g seconds" % (time.time() - start_time))
