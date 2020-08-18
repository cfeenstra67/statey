import statey as st

session = st.create_session()

session.resolve(st.Object('a,b,c').split(','))
