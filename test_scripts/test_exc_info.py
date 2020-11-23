import sys


try:
    assert False
finally:
    print("HERE", sys.exc_info())
