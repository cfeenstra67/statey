import json
import os
import textwrap as tw

import statey as st

from statey.lib import sos

from statey.provider import ProviderId


def resources(session):

    text = 'Hello, world!'

    for i in range(1, 101):
        ref = session[f'file{i}'] << sos.File(
            location=f'./tmp/test{i}.txt',
            data=text + f'\n(copied x{i})'
        )
        text = ref.data


def session():
    session = st.create_resource_session()
    resources(session)
    return session
