# import sys


# f = sys._getframe(0)
# print("HERE", f, dir(f), f.f_lineno, f.f_code, dir(f.f_code), f.f_code.co_filename, f.f_code.co_name)

import statey as st


obj = st.Object(1)

print("FRAME", obj._frame)

obj2 = st.Object(2)

print("FRAME2", obj2._frame)
