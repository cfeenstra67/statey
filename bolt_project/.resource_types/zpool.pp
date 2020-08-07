# This file was automatically generated on 2020-08-03 00:01:49 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Manage zpools. Create and delete zpools. The provider WILL NOT SYNC, only report differences.
# 
# Supports vdevs with mirrors, raidz, logs and spares.
# 
# @example Using zpool.
#   zpool { 'tstpool':
#     ensure => present,
#     disk => '/ztstpool/dsk',
#   }
Puppet::Resource::ResourceType3.new(
  'zpool',
  [
    # The basic property that the resource should be in.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # The disk(s) for this pool. Can be an array or a space separated string.
    Puppet::Resource::Param(Any, 'disk'),

    # List of all the devices to mirror for this pool. Each mirror should be a
    # space separated string:
    # 
    #     mirror => ["disk1 disk2", "disk3 disk4"],
    Puppet::Resource::Param(Any, 'mirror'),

    # List of all the devices to raid for this pool. Should be an array of
    # space separated strings:
    # 
    #     raidz => ["disk1 disk2", "disk3 disk4"],
    Puppet::Resource::Param(Any, 'raidz'),

    # Spare disk(s) for this pool.
    Puppet::Resource::Param(Any, 'spare'),

    # Log disks for this pool. This type does not currently support mirroring of log disks.
    Puppet::Resource::Param(Any, 'log')
  ],
  [
    # The name for this pool.
    Puppet::Resource::Param(Any, 'pool', true),

    # Determines parity when using the `raidz` parameter.
    Puppet::Resource::Param(Any, 'raid_parity'),

    # The specific backend to use for this `zpool`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # zpool
    # : Provider for zpool.
    # 
    #   * Required binaries: `zpool`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['pool']
  },
  true,
  false)
