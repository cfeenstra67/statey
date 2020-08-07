# This file was automatically generated on 2020-08-03 00:01:47 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Installs and manages host entries.  For most systems, these
# entries will just be in `/etc/hosts`, but some systems (notably OS X)
# will have different solutions.
Puppet::Resource::ResourceType3.new(
  'host',
  [
    # The basic property that the resource should be in.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # The host's IP address, IPv4 or IPv6.
    Puppet::Resource::Param(Any, 'ip'),

    # Any aliases the host might have.  Multiple values must be
    # specified as an array.
    Puppet::Resource::Param(Any, 'host_aliases'),

    # A comment that will be attached to the line with a # character.
    Puppet::Resource::Param(Any, 'comment'),

    # The file in which to store service information.  Only used by
    # those providers that write to disk. On most systems this defaults to `/etc/hosts`.
    Puppet::Resource::Param(Any, 'target')
  ],
  [
    # The host name.
    Puppet::Resource::Param(Any, 'name', true),

    # The specific backend to use for this `host`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # parsed
    # : Installs and manages host entries.  For most systems, these
    #   entries will just be in `/etc/hosts`, but some systems (notably OS X)
    #   will have different solutions.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
