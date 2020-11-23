import statey as st


provider = st.registry.get_provider('default')

from statey.provider import default_provider

print("HERE", provider, provider == default_provider)

print("FILE", provider.get_resource('file'))
