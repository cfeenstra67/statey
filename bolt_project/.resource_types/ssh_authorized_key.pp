# This file was automatically generated on 2020-08-03 00:01:48 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Manages SSH authorized keys. Currently only type 2 keys are supported.
# 
# In their native habitat, SSH keys usually appear as a single long line, in
# the format `<TYPE> <KEY> <NAME/COMMENT>`. This resource type requires you
# to split that line into several attributes. Thus, a key that appears in
# your `~/.ssh/id_rsa.pub` file like this...
# 
#     ssh-rsa AAAAB3Nza[...]qXfdaQ== nick@magpie.example.com
# 
# ...would translate to the following resource:
# 
#     ssh_authorized_key { 'nick@magpie.example.com':
#       ensure => present,
#       user   => 'nick',
#       type   => 'ssh-rsa',
#       key    => 'AAAAB3Nza[...]qXfdaQ==',
#     }
# 
# To ensure that only the currently approved keys are present, you can purge
# unmanaged SSH keys on a per-user basis. Do this with the `user` resource
# type's `purge_ssh_keys` attribute:
# 
#     user { 'nick':
#       ensure         => present,
#       purge_ssh_keys => true,
#     }
# 
# This will remove any keys in `~/.ssh/authorized_keys` that aren't being
# managed with `ssh_authorized_key` resources. See the documentation of the
# `user` type for more details.
# 
# **Autorequires:** If Puppet is managing the user account in which this
# SSH key should be installed, the `ssh_authorized_key` resource will autorequire
# that user.
Puppet::Resource::ResourceType3.new(
  'ssh_authorized_key',
  [
    # The basic property that the resource should be in.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # The encryption type used.
    # 
    # Valid values are `ssh-dss` (also called `dsa`), `ssh-rsa` (also called `rsa`), `ecdsa-sha2-nistp256`, `ecdsa-sha2-nistp384`, `ecdsa-sha2-nistp521`, `ssh-ed25519` (also called `ed25519`).
    Puppet::Resource::Param(Enum['ssh-dss', 'dsa', 'ssh-rsa', 'rsa', 'ecdsa-sha2-nistp256', 'ecdsa-sha2-nistp384', 'ecdsa-sha2-nistp521', 'ssh-ed25519', 'ed25519'], 'type'),

    # The public key itself; generally a long string of hex characters. The `key`
    # attribute may not contain whitespace.
    # 
    # Make sure to omit the following in this attribute (and specify them in
    # other attributes):
    # 
    # * Key headers, such as 'ssh-rsa' --- put these in the `type` attribute.
    # * Key identifiers / comments, such as 'joe@joescomputer.local' --- put these in
    #   the `name` attribute/resource title.
    Puppet::Resource::Param(Any, 'key'),

    # The user account in which the SSH key should be installed. The resource
    # will autorequire this user if it is being managed as a `user` resource.
    Puppet::Resource::Param(Any, 'user'),

    # The absolute filename in which to store the SSH key. This
    # property is optional and should be used only in cases where keys
    # are stored in a non-standard location, for instance when not in
    # `~user/.ssh/authorized_keys`. The parent directory must be present
    # if the target is in a privileged path.
    Puppet::Resource::Param(Any, 'target'),

    # Key options; see sshd(8) for possible values. Multiple values
    # should be specified as an array. For example, you could use the
    # following to install a SSH CA that allows someone with the
    # 'superuser' principal to log in as root
    # 
    #      ssh_authorized_key { 'Company SSH CA':
    #        ensure  => present,
    #        user    => 'root',
    #        type    => 'ssh-ed25519',
    #        key     => 'AAAAC3NzaC[...]CeA5kG',
    #        options => [ 'cert-authority', 'principals="superuser"' ],
    #      }
    Puppet::Resource::Param(Any, 'options')
  ],
  [
    # The SSH key comment. This can be anything, and doesn't need to match
    # the original comment from the `.pub` file.
    # 
    # Due to internal limitations, this must be unique across all user accounts;
    # if you want to specify one key for multiple users, you must use a different
    # comment for each instance.
    Puppet::Resource::Param(Any, 'name', true),

    # Whether to drop privileges when writing the key file. This is
    # useful for creating files in paths not writable by the target user. Note
    # the possible security implications of managing file ownership and
    # permissions as a privileged user.
    # 
    # Valid values are `true`, `false`, `yes`, `no`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false', 'yes', 'no']], 'drop_privileges'),

    # The specific backend to use for this `ssh_authorized_key`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # parsed
    # : Parse and generate authorized_keys files for SSH.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
