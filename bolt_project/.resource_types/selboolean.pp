# This file was automatically generated on 2020-08-03 00:01:48 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Manages SELinux booleans on systems with SELinux support.  The supported booleans
# are any of the ones found in `/selinux/booleans/`.
Puppet::Resource::ResourceType3.new(
  'selboolean',
  [
    # Whether the SELinux boolean should be enabled or disabled.
    # Valid values are: "on"/"true"/"off"/"false"
    # 
    # Valid values are `on`, `off`, `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['on', 'off', 'true', 'false']], 'value')
  ],
  [
    # The name of the SELinux boolean to be managed.
    Puppet::Resource::Param(Any, 'name', true),

    # If set true, SELinux booleans will be written to disk and persist across reboots.
    # The default is `false`.
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'persistent'),

    # The specific backend to use for this `selboolean`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # getsetsebool
    # : Manage SELinux booleans using the getsebool and setsebool binaries.
    # 
    #   * Required binaries: `/usr/sbin/getsebool`, `/usr/sbin/setsebool`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
