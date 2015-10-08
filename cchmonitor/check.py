import time

def run_test(name, func, args, timeout=None):
    result = [name]
    timed_out = False
    start_time = time.time()
    result.append(func(*args))
    end_time = time.time()
    duration = end_time - start_time
    if timeout:
        timed_out = duration<timeout
    result += [duration, timed_out, func.__name__]
    return result

def push_test(subject, results):
    # TODO: Improve output format
    def dump_result(result):
        print '=============================='
        print 'Type: %s' % result[0]
        print 'Test: %s' % result[4]
        print 'Time: %s' % result[2]
        print 'Timeout: %s' % result[3]
        print 'Output: ' 
        if not isinstance(result[1], list):
            print result[1] 
        else:
            for r in result[1]:
                print r
    for result in results:
        dump_result(result)
