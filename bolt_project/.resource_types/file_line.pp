# This file was automatically generated on 2020-08-03 00:01:39 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# @summary
#   Ensures that a given line is contained within a file.
# 
# The implementation matches the full line, including whitespace at the
# beginning and end.  If the line is not contained in the given file, Puppet
# will append the line to the end of the file to ensure the desired state.
# Multiple resources may be declared to manage multiple lines in the same file.
# 
# * Ensure Example
# ```
# file_line { 'sudo_rule':
#   path => '/etc/sudoers',
#   line => '%sudo ALL=(ALL) ALL',
# }
# 
# file_line { 'sudo_rule_nopw':
#   path => '/etc/sudoers',
#   line => '%sudonopw ALL=(ALL) NOPASSWD: ALL',
# }
# ```
# In this example, Puppet will ensure both of the specified lines are
# contained in the file /etc/sudoers.
# 
# * Match Example
# 
# ```
# file_line { 'bashrc_proxy':
#   ensure => present,
#   path   => '/etc/bashrc',
#   line   => 'export HTTP_PROXY=http://squid.puppetlabs.vm:3128',
#   match  => '^export HTTP_PROXY=',
# }
# ```
# 
# In this code example match will look for a line beginning with export
# followed by HTTP_PROXY and replace it with the value in line.
# 
# * Examples With `ensure => absent`:
# 
# This type has two behaviors when `ensure => absent` is set.
# 
# One possibility is to set `match => ...` and `match_for_absence => true`,
# as in the following example:
# 
# ```
# file_line { 'bashrc_proxy':
#   ensure            => absent,
#   path              => '/etc/bashrc',
#   match             => '^export HTTP_PROXY=',
#   match_for_absence => true,
# }
# ```
# 
# In this code example match will look for a line beginning with export
# followed by HTTP_PROXY and delete it.  If multiple lines match, an
# error will be raised unless the `multiple => true` parameter is set.
# 
# Note that the `line => ...` parameter would be accepted BUT IGNORED in
# the above example.
# 
# The second way of using `ensure => absent` is to specify a `line => ...`,
# and no match:
# 
# ```
# file_line { 'bashrc_proxy':
#   ensure => absent,
#   path   => '/etc/bashrc',
#   line   => 'export HTTP_PROXY=http://squid.puppetlabs.vm:3128',
# }
# ```
# 
# > *Note:*
# When ensuring lines are absent this way, the default behavior
# this time is to always remove all lines matching, and this behavior
# can't be disabled.
# 
# * Encoding example:
# 
# ```
# file_line { "XScreenSaver":
#   ensure   => present,
#   path     => '/root/XScreenSaver',
#   line     => "*lock: 10:00:00",
#   match    => '^*lock:',
#   encoding => "iso-8859-1",
# }
# ```
# 
# Files with special characters that are not valid UTF-8 will give the
# error message "invalid byte sequence in UTF-8".  In this case, determine
# the correct file encoding and specify the correct encoding using the
# encoding attribute, the value of which needs to be a valid Ruby character
# encoding.
# 
# **Autorequires:** If Puppet is managing the file that will contain the line
# being managed, the file_line resource will autorequire that file.
Puppet::Resource::ResourceType3.new(
  'file_line',
  [
    # Manage the state of this type.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # The line to be appended to the file or used to replace matches found by the match attribute.
    Puppet::Resource::Param(Any, 'line')
  ],
  [
    # An arbitrary name used as the identity of the resource.
    Puppet::Resource::Param(Any, 'name', true),

    # An optional ruby regular expression to run against existing lines in the file.
    # If a match is found, we replace that line rather than adding a new line.
    # A regex comparison is performed against the line value and if it does not
    # match an exception will be raised.
    Puppet::Resource::Param(Any, 'match'),

    # An optional value to determine if match should be applied when ensure => absent.
    # If set to true and match is set, the line that matches match will be deleted.
    # If set to false (the default), match is ignored when ensure => absent.
    # When `ensure => present`, match_for_absence is ignored.
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'match_for_absence'),

    # An optional value to determine if match can change multiple lines.
    # If set to false, an exception will be raised if more than one line matches
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'multiple'),

    # An optional value used to specify the line after which we will add any new lines. (Existing lines are added in place)
    # This is also takes a regex.
    Puppet::Resource::Param(Any, 'after'),

    # The file Puppet will ensure contains the line specified by the line parameter.
    Puppet::Resource::Param(Any, 'path'),

    # If true, replace line that matches. If false, do not write line if a match is found
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'replace'),

    # Configures the behavior of replacing all lines in a file which match the `match` parameter regular expression, regardless of whether the specified line is already present in the file.
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'replace_all_matches_not_matching_line'),

    # For files that are not UTF-8 encoded, specify encoding such as iso-8859-1
    Puppet::Resource::Param(Any, 'encoding'),

    # If true, append line if match is not found. If false, do not append line if a match is not found
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'append_on_no_match'),

    # The specific backend to use for this `file_line`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # ruby
    # : @summary
    #     This type allows puppet to manage small config files.
    # 
    #   The implementation matches the full line, including whitespace at the
    #   beginning and end.  If the line is not contained in the given file, Puppet
    #   will append the line to the end of the file to ensure the desired state.
    #   Multiple resources may be declared to manage multiple lines in the same file.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
