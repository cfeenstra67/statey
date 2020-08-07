# This file was automatically generated on 2020-08-03 00:01:39 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# @summary
#   Generates a file with content from fragments sharing a common unique tag.
# 
# @example
#   Concat_fragment <<| tag == 'unique_tag' |>>
# 
#   concat_file { '/tmp/file':
#     tag            => 'unique_tag', # Optional. Default to undef
#     path           => '/tmp/file',  # Optional. If given it overrides the resource name
#     owner          => 'root',       # Optional. Default to undef
#     group          => 'root',       # Optional. Default to undef
#     mode           => '0644'        # Optional. Default to undef
#     order          => 'numeric'     # Optional, Default to 'numeric'
#     ensure_newline => false         # Optional, Defaults to false
#   }
Puppet::Resource::ResourceType3.new(
  'concat_file',
  [
    # Specifies whether the destination file should exist. Setting to 'absent' tells Puppet to delete the destination file if it exists, and
    # negates the effect of any other parameters.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure')
  ],
  [
    # Required. Specifies a unique tag reference to collect all concat_fragments with the same tag.
    Puppet::Resource::Param(Any, 'tag'),

    # Specifies a destination file for the combined fragments. Valid options: a string containing an absolute path. Default value: the
    # title of your declared resource.
    Puppet::Resource::Param(Any, 'path', true),

    # Specifies the owner of the destination file. Valid options: a string containing a username or integer containing a uid.
    Puppet::Resource::Param(Any, 'owner'),

    # Specifies a permissions group for the destination file. Valid options: a string containing a group name or integer containing a
    # gid.
    Puppet::Resource::Param(Any, 'group'),

    # Specifies the permissions mode of the destination file. Valid options: a string containing a permission mode value in octal notation.
    Puppet::Resource::Param(Any, 'mode'),

    # Specifies a method for sorting your fragments by name within the destination file. You can override this setting for individual
    # fragments by adjusting the order parameter in their concat::fragment declarations.
    # 
    # Valid values are `alpha`, `numeric`.
    Puppet::Resource::Param(Enum['alpha', 'numeric'], 'order'),

    # Specifies whether (and how) to back up the destination file before overwriting it. Your value gets passed on to Puppet's native file
    # resource for execution. Valid options: true, false, or a string representing either a target filebucket or a filename extension
    # beginning with ".".'
    Puppet::Resource::Param(Any, 'backup'),

    # Specifies whether to overwrite the destination file if it already exists.
    # 
    # Valid values are `true`, `false`, `yes`, `no`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false', 'yes', 'no']], 'replace'),

    # Specifies a validation command to apply to the destination file. Requires Puppet version 3.5 or newer. Valid options: a string to
    # be passed to a file resource.
    Puppet::Resource::Param(Any, 'validate_cmd'),

    # Specifies whether to add a line break at the end of each fragment that doesn't already end in one.
    # 
    # Valid values are `true`, `false`, `yes`, `no`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false', 'yes', 'no']], 'ensure_newline'),

    # Specify what data type to merge the fragments as. Valid options: 'plain', 'yaml', 'json', 'json-array', 'json-pretty', 'json-array-pretty'.
    # 
    # Valid values are `plain`, `yaml`, `json`, `json-array`, `json-pretty`, `json-array-pretty`.
    Puppet::Resource::Param(Enum['plain', 'yaml', 'json', 'json-array', 'json-pretty', 'json-array-pretty'], 'format'),

    # Specifies whether to merge data structures, keeping the values with higher order.
    # 
    # Valid values are `true`, `false`, `yes`, `no`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false', 'yes', 'no']], 'force'),

    # See the file type's selinux_ignore_defaults documentention:
    # https://docs.puppetlabs.com/references/latest/type.html#file-attribute-selinux_ignore_defaults.
    # 
    # Valid values are `true`, `false`, `yes`, `no`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false', 'yes', 'no']], 'selinux_ignore_defaults'),

    # See the file type's selrange documentation: https://docs.puppetlabs.com/references/latest/type.html#file-attribute-selrange
    Puppet::Resource::Param(Any, 'selrange'),

    # See the file type's selrole documentation: https://docs.puppetlabs.com/references/latest/type.html#file-attribute-selrole
    Puppet::Resource::Param(Any, 'selrole'),

    # See the file type's seltype documentation: https://docs.puppetlabs.com/references/latest/type.html#file-attribute-seltype
    Puppet::Resource::Param(Any, 'seltype'),

    # See the file type's seluser documentation: https://docs.puppetlabs.com/references/latest/type.html#file-attribute-seluser
    Puppet::Resource::Param(Any, 'seluser'),

    # Specifies whether to set the show_diff parameter for the file resource. Useful for hiding secrets stored in hiera from insecure
    # reporting methods.
    # 
    # Valid values are `true`, `false`, `yes`, `no`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false', 'yes', 'no']], 'show_diff')
  ],
  {
    /(?m-ix:(.*))/ => ['path']
  },
  true,
  false)
