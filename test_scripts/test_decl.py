from __future__ import annotations

import statey as st
from typing import Optional


@st.declarative
def module(session):
	a = 1
	b = 2
	c = session["c": int] << 3
	d = st.Object(4, int)
	e = st.Object(None, Optional[str])
	print("HERE", a, b, c, d)


sess = st.create_session()

module(sess)

print("DATA", sess.data)
