# This file was automatically generated on 2020-08-03 00:01:48 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Manages loading and unloading of SELinux policy modules
# on the system.  Requires SELinux support.  See man semodule(8)
# for more information on SELinux policy modules.
# 
# **Autorequires:** If Puppet is managing the file containing this SELinux
# policy module (which is either explicitly specified in the `selmodulepath`
# attribute or will be found at {`selmoduledir`}/{`name`}.pp), the selmodule
# resource will autorequire that file.
Puppet::Resource::ResourceType3.new(
  'selmodule',
  [
    # The basic property that the resource should be in.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # If set to `true`, the policy will be reloaded if the
    # version found in the on-disk file differs from the loaded
    # version.  If set to `false` (the default) the only check
    # that will be made is if the policy is loaded at all or not.
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'syncversion')
  ],
  [
    # The name of the SELinux policy to be managed.  You should not
    # include the customary trailing .pp extension.
    Puppet::Resource::Param(Any, 'name', true),

    # The directory to look for the compiled pp module file in.
    # Currently defaults to `/usr/share/selinux/targeted`.  If the
    # `selmodulepath` attribute is not specified, Puppet will expect to find
    # the module in `<selmoduledir>/<name>.pp`, where `name` is the value of the
    # `name` parameter.
    Puppet::Resource::Param(Any, 'selmoduledir'),

    # The full path to the compiled .pp policy module.  You only need to use
    # this if the module file is not in the `selmoduledir` directory.
    Puppet::Resource::Param(Any, 'selmodulepath'),

    # The specific backend to use for this `selmodule`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # semodule
    # : Manage SELinux policy modules using the semodule binary.
    # 
    #   * Required binaries: `/usr/sbin/semodule`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
