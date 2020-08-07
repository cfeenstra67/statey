# This file was automatically generated on 2020-08-03 00:01:40 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Apply a change or an array of changes to the filesystem
# using the augeas tool.
# 
# Requires:
# 
# - [Augeas](http://www.augeas.net)
# - The ruby-augeas bindings
# 
# Sample usage with a string:
# 
#     augeas{"test1" :
#       context => "/files/etc/sysconfig/firstboot",
#       changes => "set RUN_FIRSTBOOT YES",
#       onlyif  => "match other_value size > 0",
#     }
# 
# Sample usage with an array and custom lenses:
# 
#     augeas{"jboss_conf":
#       context   => "/files",
#       changes   => [
#           "set etc/jbossas/jbossas.conf/JBOSS_IP $ipaddress",
#           "set etc/jbossas/jbossas.conf/JAVA_HOME /usr",
#         ],
#       load_path => "$/usr/share/jbossas/lenses",
#     }
Puppet::Resource::ResourceType3.new(
  'augeas',
  [
    # The expected return code from the augeas command. Should not be set.
    Puppet::Resource::Param(Any, 'returns')
  ],
  [
    # The name of this task. Used for uniqueness.
    Puppet::Resource::Param(Any, 'name', true),

    # Optional context path. This value is prepended to the paths of all
    # changes if the path is relative. If the `incl` parameter is set,
    # defaults to `/files + incl`; otherwise, defaults to the empty string.
    Puppet::Resource::Param(Any, 'context'),

    # Optional augeas command and comparisons to control the execution of this type.
    # 
    # Note: `values` is not an actual augeas API command. It calls `match` to retrieve an array of paths
    #        in <MATCH_PATH> and then `get` to retrieve the values from each of the returned paths.
    # 
    # Supported onlyif syntax:
    # 
    # * `get <AUGEAS_PATH> <COMPARATOR> <STRING>`
    # * `values <MATCH_PATH> include <STRING>`
    # * `values <MATCH_PATH> not_include <STRING>`
    # * `values <MATCH_PATH> == <AN_ARRAY>`
    # * `values <MATCH_PATH> != <AN_ARRAY>`
    # * `match <MATCH_PATH> size <COMPARATOR> <INT>`
    # * `match <MATCH_PATH> include <STRING>`
    # * `match <MATCH_PATH> not_include <STRING>`
    # * `match <MATCH_PATH> == <AN_ARRAY>`
    # * `match <MATCH_PATH> != <AN_ARRAY>`
    # 
    # where:
    # 
    # * `AUGEAS_PATH` is a valid path scoped by the context
    # * `MATCH_PATH` is a valid match syntax scoped by the context
    # * `COMPARATOR` is one of `>, >=, !=, ==, <=,` or `<`
    # * `STRING` is a string
    # * `INT` is a number
    # * `AN_ARRAY` is in the form `['a string', 'another']`
    Puppet::Resource::Param(Any, 'onlyif'),

    # The changes which should be applied to the filesystem. This
    # can be a command or an array of commands. The following commands are supported:
    # 
    # * `set <PATH> <VALUE>` --- Sets the value `VALUE` at location `PATH`
    # * `setm <PATH> <SUB> <VALUE>` --- Sets multiple nodes (matching `SUB` relative to `PATH`) to `VALUE`
    # * `rm <PATH>` --- Removes the node at location `PATH`
    # * `remove <PATH>` --- Synonym for `rm`
    # * `clear <PATH>` --- Sets the node at `PATH` to `NULL`, creating it if needed
    # * `clearm <PATH> <SUB>` --- Sets multiple nodes (matching `SUB` relative to `PATH`) to `NULL`
    # * `touch <PATH>` --- Creates `PATH` with the value `NULL` if it does not exist
    # * `ins <LABEL> (before|after) <PATH>` --- Inserts an empty node `LABEL` either before or after `PATH`.
    # * `insert <LABEL> <WHERE> <PATH>` --- Synonym for `ins`
    # * `mv <PATH> <OTHER PATH>` --- Moves a node at `PATH` to the new location `OTHER PATH`
    # * `move <PATH> <OTHER PATH>` --- Synonym for `mv`
    # * `rename <PATH> <LABEL>` --- Rename a node at `PATH` to a new `LABEL`
    # * `defvar <NAME> <PATH>` --- Sets Augeas variable `$NAME` to `PATH`
    # * `defnode <NAME> <PATH> <VALUE>` --- Sets Augeas variable `$NAME` to `PATH`, creating it with `VALUE` if needed
    # 
    # If the `context` parameter is set, that value is prepended to any relative `PATH`s.
    Puppet::Resource::Param(Any, 'changes'),

    # A file system path; all files loaded by Augeas are loaded underneath `root`.
    Puppet::Resource::Param(Any, 'root'),

    # Optional colon-separated list or array of directories; these directories are searched for schema definitions. The agent's `$libdir/augeas/lenses` path will always be added to support pluginsync.
    Puppet::Resource::Param(Any, 'load_path'),

    # Optional command to force the augeas type to execute even if it thinks changes
    # will not be made. This does not override the `onlyif` parameter.
    Puppet::Resource::Param(Any, 'force'),

    # Whether augeas should perform typechecking. Defaults to false.
    # 
    # Valid values are `true`, `false`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false']], 'type_check'),

    # Use a specific lens, such as `Hosts.lns`. When this parameter is set, you
    # must also set the `incl` parameter to indicate which file to load.
    # The Augeas documentation includes [a list of available lenses](http://augeas.net/stock_lenses.html).
    Puppet::Resource::Param(Any, 'lens'),

    # Load only a specific file, such as `/etc/hosts`. This can greatly speed
    # up the execution the resource. When this parameter is set, you must also
    # set the `lens` parameter to indicate which lens to use.
    Puppet::Resource::Param(Any, 'incl'),

    # Whether to display differences when the file changes, defaulting to
    # true.  This parameter is useful for files that may contain passwords or
    # other secret data, which might otherwise be included in Puppet reports or
    # other insecure outputs.  If the global `show_diff` setting
    # is false, then no diffs will be shown even if this parameter is true.
    # 
    # Valid values are `true`, `false`, `yes`, `no`.
    Puppet::Resource::Param(Variant[Boolean, Enum['true', 'false', 'yes', 'no']], 'show_diff'),

    # The specific backend to use for this `augeas`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # augeas
    # : * Supported features: `execute_changes`, `need_to_run?`, `parse_commands`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
