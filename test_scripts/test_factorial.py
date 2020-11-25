import contextlib
import time

import statey as st


NUM = 100


@contextlib.contextmanager
def timer():
    start = time.time()
    try:
        yield
    finally:
        print("Time: %.4fs" % (time.time() - start))


def fibonacci(i):
    a, b = 0, 1
    if i == 0:
        return a

    idx = 1
    while idx < i:
        a, b = b, a + b
        idx += 1

    return b


session = st.create_session()

import sys
sys.setrecursionlimit(100)


a = session["0"] << 0
b = session["1"] << 1

for i in range(2, NUM + 1):
    c = session[str(i)] << a + b
    a, b = b, c


with timer():
    print("COMPARISON", fibonacci(NUM))

with timer():
    print("SESSION", session.resolve(b))


# @st.function
# def get_ref() -> str:
#     return st.Object(st.syms.impl.Reference('a', session.ns), str)


# ref1 = session["b"] << get_ref()
# ref2 = session["a"] << st.Object(st.syms.impl.Reference('b', session.ns), str)

# with timer():
#     print("CIRCLE", session.resolve(ref2))
