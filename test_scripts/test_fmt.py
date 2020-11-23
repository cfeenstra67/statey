import statey as st


session = st.create_session()

a = 1

b = st.Object({'a': 123})

c = session['c': st.Map[str, str]] << {'blah': 'eff'}

s = st.f('a = {a}, b = {b}, c.blah = {c.blah}')

print("RESOLVED", session.resolve(s))
