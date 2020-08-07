# This file was automatically generated on 2020-08-03 00:01:47 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Manages mounted filesystems, including putting mount
# information into the mount table. The actual behavior depends
# on the value of the 'ensure' parameter.
# 
# **Refresh:** `mount` resources can respond to refresh events (via
# `notify`, `subscribe`, or the `~>` arrow). If a `mount` receives an event
# from another resource **and** its `ensure` attribute is set to `mounted`,
# Puppet will try to unmount then remount that filesystem.
# 
# **Autorequires:** If Puppet is managing any parents of a mount resource ---
# that is, other mount points higher up in the filesystem --- the child
# mount will autorequire them.
# 
# **Autobefores:**  If Puppet is managing any child file paths of a mount
# point, the mount resource will autobefore them.
Puppet::Resource::ResourceType3.new(
  'mount',
  [
    # Control what to do with this mount. Set this attribute to
    # `unmounted` to make sure the filesystem is in the filesystem table
    # but not mounted (if the filesystem is currently mounted, it will be
    # unmounted).  Set it to `absent` to unmount (if necessary) and remove
    # the filesystem from the fstab.  Set to `mounted` to add it to the
    # fstab and mount it. Set to `present` to add to fstab but not change
    # mount/unmount status.
    # 
    # Valid values are `defined` (also called `present`), `unmounted`, `absent`, `mounted`.
    Puppet::Resource::Param(Enum['defined', 'present', 'unmounted', 'absent', 'mounted'], 'ensure'),

    # The device providing the mount.  This can be whatever device
    # is supporting by the mount, including network devices or
    # devices specified by UUID rather than device path, depending
    # on the operating system. On Linux systems it can contain
    # whitespace.
    Puppet::Resource::Param(Any, 'device'),

    # The device to fsck.  This is property is only valid
    # on Solaris, and in most cases will default to the correct
    # value.
    Puppet::Resource::Param(Any, 'blockdevice'),

    # The mount type.  Valid values depend on the
    # operating system.  This is a required option.
    Puppet::Resource::Param(Any, 'fstype'),

    # A single string containing options for the mount, as they would
    # appear in fstab on Linux. For many platforms this is a comma-delimited
    # string. Consult the fstab(5) man page for system-specific details.
    # AIX options other than dev, nodename, or vfs can be defined here. If
    # specified, AIX options of account, boot, check, free, mount, size,
    # type, vol, log, and quota must be ordered alphabetically at the end of
    # the list.
    Puppet::Resource::Param(Any, 'options'),

    # The pass in which the mount is checked.
    Puppet::Resource::Param(Any, 'pass'),

    # Whether to mount the mount at boot.  Not all platforms
    # support this.
    Puppet::Resource::Param(Any, 'atboot'),

    # Whether to dump the mount.  Not all platform support this.
    # Valid values are `1` or `0` (or `2` on FreeBSD). Default is `0`.
    # 
    # Values can match `/(0|1)/`.
    Puppet::Resource::Param(Pattern[/(0|1)/], 'dump'),

    # The file in which to store the mount table.  Only used by
    # those providers that write to disk.
    Puppet::Resource::Param(Any, 'target')
  ],
  [
    # The mount path for the mount. On Linux systems it can contain whitespace.
    Puppet::Resource::Param(Any, 'name', true),

    # Whether the mount can be remounted  `mount -o remount`.  If
    # this is false, then the filesystem will be unmounted and remounted
    # manually, which is prone to failure.
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'remounts'),

    # The specific backend to use for this `mount`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # parsed
    # : Installs and manages host entries.  For most systems, these
    #   entries will just be in `/etc/hosts`, but some systems (notably OS X)
    #   will have different solutions.
    # 
    #   * Required binaries: `mount`, `umount`.
    #   * Supported features: `refreshable`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
