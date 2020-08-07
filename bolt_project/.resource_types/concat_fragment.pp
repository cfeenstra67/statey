# This file was automatically generated on 2020-08-03 00:01:39 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# @summary
#   Manages the fragment.
# 
# @example
#   # The example is based on exported resources.
# 
#   concat_fragment { "uniqe_name_${::fqdn}":
#     tag => 'unique_name',
#     order => 10, # Optional. Default to 10
#     content => 'some content' # OR
#     # content => template('template.erb')
#     source  => 'puppet:///path/to/file'
#   }
Puppet::Resource::ResourceType3.new(
  'concat_fragment',
  [

  ],
  [
    # Name of resource.
    Puppet::Resource::Param(Any, 'name', true),

    # Required. Specifies the destination file of the fragment. Valid options: a string containing the path or title of the parent
    # concat_file resource.
    Puppet::Resource::Param(Any, 'target'),

    # Supplies the content of the fragment. Note: You must supply either a content parameter or a source parameter. Valid options: a string
    Puppet::Resource::Param(Any, 'content'),

    # Specifies a file to read into the content of the fragment. Note: You must supply either a content parameter or a source parameter.
    # Valid options: a string or an array, containing one or more Puppet URLs.
    Puppet::Resource::Param(Any, 'source'),

    # Reorders your fragments within the destination file. Fragments that share the same order number are ordered by name. The string
    # option is recommended.
    Puppet::Resource::Param(Any, 'order'),

    # Specifies a unique tag to be used by concat_file to reference and collect content.
    Puppet::Resource::Param(Any, 'tag')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
