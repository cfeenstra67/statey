# This file was automatically generated on 2020-08-03 00:01:48 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Manages system reboots.  The `reboot` type is typically
# used in situations where a resource performs a change, e.g.
# package install, and a reboot is required to complete
# installation.  Only if the package is installed should the
# reboot be triggered.
# 
# Sample usage:
# 
#     package { 'Microsoft .NET Framework 4.5':
#       ensure          => installed,
#       source          => '\\server\share\dotnetfx45_full_x86_x64.exe',
#       install_options => ['/Passive', '/NoRestart'],
#       provider        => windows,
#     }
#     reboot { 'after':
#       subscribe       => Package['Microsoft .NET Framework 4.5'],
#     }
# 
# A reboot resource can also check if the system is in a
# reboot pending state, and if so, reboot the system.  For
# example, if you have a package that cannot be installed
# while a reboot is pending.
# 
# Sample usage:
# 
#     reboot { 'before':
#       when            => pending,
#     }
#     package { 'Microsoft .NET Framework 4.5':
#       ensure          => installed,
#       source          => '\\server\share\dotnetfx45_full_x86_x64.exe',
#       install_options => ['/Passive', '/NoRestart'],
#       provider        => windows,
#       require         => Reboot['before'],
#     }
# 
# A reboot resource can also finish the run and then reboot the system.  For
# example, if you have a few packages that all require reboots but will not block
# each other during the run.
# 
# Sample usage:
# 
#     package { 'Microsoft .NET Framework 4.5':
#       ensure          => installed,
#       source          => '\\server\share\dotnetfx45_full_x86_x64.exe',
#       install_options => ['/Passive', '/NoRestart'],
#       provider        => windows,
#     }
#     reboot { 'after_run':
#       apply           => finished,
#       subscribe       => Package['Microsoft .NET Framework 4.5'],
#     }
# 
# On windows we can limit the reasons for a pending reboot.
# 
# Sample usage:
# 
#     reboot { 'renames only':
#       when            => pending,
#       onlyif          => 'pending_rename_file_operations',
#     }
Puppet::Resource::ResourceType3.new(
  'reboot',
  [
    # When to check for, and if needed, perform a reboot. If `pending`,
    # then the provider will check if a reboot is pending, and only
    # if needed, reboot the system.  If `refreshed` then the reboot
    # will only be performed in response to a refresh event from
    # another resource, e.g. `package`.
    # 
    # Valid values are `refreshed`, `pending`.
    Puppet::Resource::Param(Enum['refreshed', 'pending'], 'when')
  ],
  [
    # The name of the reboot resource.  Used for uniqueness.
    Puppet::Resource::Param(Any, 'name', true),

    # For pending reboots, only reboot if the reboot is pending
    # for one of the supplied reasons.
    # 
    # 
    # 
    # Requires features manages_reboot_pending.
    Puppet::Resource::Param(Any, 'onlyif'),

    # For pending reboots, ignore the supplied reasons when checking pennding reboot
    # 
    # 
    # 
    # Requires features manages_reboot_pending.
    Puppet::Resource::Param(Any, 'unless'),

    # When to apply the reboot. If `immediately`, then the provider
    # will stop applying additional resources and apply the reboot once
    # puppet has finished syncing. If `finished`, it will continue
    # applying resources and then perform a reboot at the end of the
    # run. The default is `immediately`.
    # 
    # Valid values are `immediately`, `finished`.
    Puppet::Resource::Param(Enum['immediately', 'finished'], 'apply'),

    # The message to log when the reboot is performed.
    Puppet::Resource::Param(Any, 'message'),

    # The amount of time in seconds to wait between the time the reboot
    # is requested and when the reboot is performed.  The default timeout
    # is 60 seconds.  Note that this time starts once puppet has exited the
    # current run.
    Puppet::Resource::Param(Any, 'timeout'),

    # The specific backend to use for this `reboot`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # linux
    # : 
    # 
    # posix
    # : POSIX provider for the reboot type.
    # 
    #   This provider handles rebooting for POSIX systems. It does not support
    #   HP-UX.
    # 
    #   * Required binaries: `shutdown`.
    #   * Default for `feature` == `posix`.
    # 
    # windows
    # : * Required binaries: `shutdown.exe`.
    #   * Default for `operatingsystem` == `windows`.
    #   * Supported features: `manages_reboot_pending`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
