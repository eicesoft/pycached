#coding=utf-8
if __name__ == '__main__':
    max = 100000

    import random, string,os, functools, time
    def timeit(func):
        @functools.wraps(func)
        def __do__(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            print '%s used time %0.4f ms' % (func.__name__, (time.time() - start) * 1000)
            return result

        return __do__

    @timeit
    def test1():
        for i in xrange(max):
            ''.join(random.sample(string.ascii_letters, 16))

    @timeit
    def test2():
        for i in xrange(max):
            ''.join(map(lambda xx:(hex(ord(xx))[2:]),os.urandom(16)))

    @timeit
    def test3():
        for i in xrange(max):
            ''.join([(string.ascii_letters+string.digits)[x] for x in random.sample(range(0,62), 16)])


    test1()
    test2()
    test3()