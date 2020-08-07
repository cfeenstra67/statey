# This file was automatically generated on 2020-08-03 00:01:49 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Manage zfs. Create destroy and set properties on zfs instances.
# 
# **Autorequires:** If Puppet is managing the zpool at the root of this zfs
# instance, the zfs resource will autorequire it. If Puppet is managing any
# parent zfs instances, the zfs resource will autorequire them.
# 
# @example Using zfs.
#   zfs { 'tstpool':
#     ensure => present,
#   }
Puppet::Resource::ResourceType3.new(
  'zfs',
  [
    # The basic property that the resource should be in.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # The aclinherit property. Valid values are `discard`, `noallow`, `restricted`, `passthrough`, `passthrough-x`.
    Puppet::Resource::Param(Any, 'aclinherit'),

    # The aclmode property. Valid values are `discard`, `groupmask`, `passthrough`.
    Puppet::Resource::Param(Any, 'aclmode'),

    # The acltype propery. Valid values are 'noacl' and 'posixacl'. Only supported on Linux.
    Puppet::Resource::Param(Any, 'acltype'),

    # The atime property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'atime'),

    # The canmount property. Valid values are `on`, `off`, `noauto`.
    Puppet::Resource::Param(Any, 'canmount'),

    # The checksum property. Valid values are `on`, `off`, `fletcher2`, `fletcher4`, `sha256`.
    Puppet::Resource::Param(Any, 'checksum'),

    # The compression property. Valid values are `on`, `off`, `lzjb`, `gzip`, `gzip-[1-9]`, `zle`.
    Puppet::Resource::Param(Any, 'compression'),

    # The copies property. Valid values are `1`, `2`, `3`.
    Puppet::Resource::Param(Any, 'copies'),

    # The dedup property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'dedup'),

    # The devices property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'devices'),

    # The exec property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'exec'),

    # The logbias property. Valid values are `latency`, `throughput`.
    Puppet::Resource::Param(Any, 'logbias'),

    # The mountpoint property. Valid values are `<path>`, `legacy`, `none`.
    Puppet::Resource::Param(Any, 'mountpoint'),

    # The nbmand property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'nbmand'),

    # The overlay property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'overlay'),

    # The primarycache property. Valid values are `all`, `none`, `metadata`.
    Puppet::Resource::Param(Any, 'primarycache'),

    # The quota property. Valid values are `<size>`, `none`.
    Puppet::Resource::Param(Any, 'quota'),

    # The readonly property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'readonly'),

    # The recordsize property. Valid values are powers of two between 512 and 128k.
    Puppet::Resource::Param(Any, 'recordsize'),

    # The refquota property. Valid values are `<size>`, `none`.
    Puppet::Resource::Param(Any, 'refquota'),

    # The refreservation property. Valid values are `<size>`, `none`.
    Puppet::Resource::Param(Any, 'refreservation'),

    # The reservation property. Valid values are `<size>`, `none`.
    Puppet::Resource::Param(Any, 'reservation'),

    # The secondarycache property. Valid values are `all`, `none`, `metadata`.
    Puppet::Resource::Param(Any, 'secondarycache'),

    # The setuid property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'setuid'),

    # The shareiscsi property. Valid values are `on`, `off`, `type=<type>`.
    Puppet::Resource::Param(Any, 'shareiscsi'),

    # The sharenfs property. Valid values are `on`, `off`, share(1M) options
    Puppet::Resource::Param(Any, 'sharenfs'),

    # The sharesmb property. Valid values are `on`, `off`, sharemgr(1M) options
    Puppet::Resource::Param(Any, 'sharesmb'),

    # The snapdir property. Valid values are `hidden`, `visible`.
    Puppet::Resource::Param(Any, 'snapdir'),

    # The version property. Valid values are `1`, `2`, `3`, `4`, `current`.
    Puppet::Resource::Param(Any, 'version'),

    # The volsize property. Valid values are `<size>`
    Puppet::Resource::Param(Any, 'volsize'),

    # The vscan property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'vscan'),

    # The xattr property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'xattr'),

    # The zoned property. Valid values are `on`, `off`.
    Puppet::Resource::Param(Any, 'zoned')
  ],
  [
    # The full name for this filesystem (including the zpool).
    Puppet::Resource::Param(Any, 'name'),

    # The specific backend to use for this `zfs`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # zfs
    # : Provider for zfs.
    # 
    #   * Required binaries: `zfs`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
