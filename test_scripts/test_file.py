import json
import textwrap as tw

import statey as st

from statey.lib import sos


from statey.provider import ProviderId


@st.declarative
def resources(session):
    file1 = sos.File(
        location='./test1.txt',
        data='blah'
    )
    file2 = sos.File(
        location=file1.location + '.copy',
        data=st.f(tw.dedent('''
            {file1.data}
            (copied)
        ''').strip())
    )
    file3 = st.BoundState(
        st.ResourceState(
            st.State("UP", sos.FileConfigType, sos.FileType),
            'file',
            ProviderId('default'),
        ),
        {
            'location': file1.location + '.copy2',
            'data': file2.data
        }
    )


def session():
    session = st.create_resource_session()
    resources(session)
    return session
