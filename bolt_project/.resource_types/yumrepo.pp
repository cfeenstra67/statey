# This file was automatically generated on 2020-08-03 00:01:49 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# The client-side description of a yum repository. Repository
# configurations are found by parsing `/etc/yum.conf` and
# the files indicated by the `reposdir` option in that file
# (see `yum.conf(5)` for details).
# 
# Most parameters are identical to the ones documented
# in the `yum.conf(5)` man page.
# 
# Continuation lines that yum supports (for the `baseurl`, for example)
# are not supported. This type does not attempt to read or verify the
# existence of files listed in the `include` attribute.
Puppet::Resource::ResourceType3.new(
  'yumrepo',
  [
    # The basic property that the resource should be in.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # A human-readable description of the repository.
    # This corresponds to the name parameter in `yum.conf(5)`.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'descr'),

    # The URL that holds the list of mirrors for this repository.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'mirrorlist'),

    # The URL for this repository. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'baseurl'),

    # Whether this repository is enabled.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'enabled'),

    # Whether to check the GPG signature on packages installed
    # from this repository.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'gpgcheck'),

    # Whether to check the GPG signature of the packages payload.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'payload_gpgcheck'),

    # Whether to check the GPG signature on repodata.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'repo_gpgcheck'),

    # The URL for the GPG key with which packages from this
    # repository are signed. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'gpgkey'),

    # Time (in seconds) after which the mirrorlist locally cached
    #       will expire.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^[0-9]+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^[0-9]+$/]], 'mirrorlist_expire'),

    # The URL of a remote file containing additional yum configuration
    # settings. Puppet does not check for this file's existence or validity.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'include'),

    # The string of package names or shell globs separated by spaces to exclude.
    # Packages that match the package name given or shell globs will never be
    # considered in updates or installs for this repo.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'exclude'),

    # The URL for the GPG CA key for this repository. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'gpgcakey'),

    # The string of package names or shell globs separated by spaces to
    # include. If this is set, only packages matching one of the package
    # names or shell globs will be considered for update or install
    # from this repository. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'includepkgs'),

    # Whether yum will allow the use of package groups for this
    # repository.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'enablegroups'),

    # The failover method for this repository; should be either
    # `roundrobin` or `priority`. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^roundrobin|priority$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^roundrobin|priority$/]], 'failovermethod'),

    # Whether HTTP/1.1 keepalive should be used with this repository.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'keepalive'),

    # Set the number of times any attempt to retrieve a file should
    #       retry before returning an error. Setting this to `0` makes yum
    #      try forever.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^[0-9]+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^[0-9]+$/]], 'retries'),

    # What to cache from this repository. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(packages|all|none)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(packages|all|none)$/]], 'http_caching'),

    # Number of seconds to wait for a connection before timing
    # out. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^\d+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^\d+$/]], 'timeout'),

    # Number of seconds after which the metadata will expire.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^([0-9]+[dhm]?|never)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^([0-9]+[dhm]?|never)$/]], 'metadata_expire'),

    # Enable or disable protection for this repository. Requires
    # that the `protectbase` plugin is installed and enabled.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'protect'),

    # Priority of this repository. Can be any integer value
    # (including negative). Requires that the `priorities` plugin
    # is installed and enabled.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^-?\d+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^-?\d+$/]], 'priority'),

    # Sets the low speed threshold in bytes per second.
    # If the server is sending data slower than this for at least
    # `timeout` seconds, Yum aborts the connection. The default is
    # `1000`.
    #   Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^\d+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^\d+$/]], 'minrate'),

    # Enable bandwidth throttling for downloads. This option
    #       can be expressed as a absolute data rate in bytes/sec or a
    #       percentage `60%`. An SI prefix (k, M or G) may be appended
    #       to the data rate values.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^\d+[kMG%]?$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^\d+[kMG%]?$/]], 'throttle'),

    # Use to specify the maximum available network bandwidth
    #       in bytes/second. Used with the `throttle` option. If `throttle`
    #       is a percentage and `bandwidth` is `0` then bandwidth throttling
    #       will be disabled. If `throttle` is expressed as a data rate then
    #       this option is ignored.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^\d+[kMG]?$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^\d+[kMG]?$/]], 'bandwidth'),

    # Cost of this repository. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^\d+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^\d+$/]], 'cost'),

    # URL of a proxy server that Yum should use when accessing this repository.
    # This attribute can also be set to '_none_' (or '' for EL >= 8 only),
    # which will make Yum bypass any global proxy settings when accessing this repository.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'proxy'),

    # Username for this proxy. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'proxy_username'),

    # Password for this proxy. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'proxy_password'),

    # Access the repository via S3.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 's3_enabled'),

    # Path to the directory containing the databases of the
    # certificate authorities yum should use to verify SSL certificates.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'sslcacert'),

    # Should yum verify SSL certificates/hosts at all.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'sslverify'),

    # Path  to the SSL client certificate yum should use to connect
    # to repositories/remote sites. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'sslclientcert'),

    # Path to the SSL client key yum should use to connect
    # to repositories/remote sites. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'sslclientkey'),

    # Metalink for mirrors. Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'metalink'),

    # Should yum skip this repository if unable to reach it.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'skip_if_unavailable'),

    # Determines if yum prompts for confirmation of critical actions.
    # Valid values are: false/0/no or true/1/yes.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^(true|false|0|1|no|yes)$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^(true|false|0|1|no|yes)$/]], 'assumeyes'),

    # Percentage value that determines when to use deltas for this repository.
    # When the delta is larger than this percentage value of the package, the
    # delta is not used.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^\d+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^\d+$/]], 'deltarpm_percentage'),

    # Percentage value that determines when to download deltarpm metadata.
    # When the deltarpm metadata is larger than this percentage value of the
    # package, deltarpm metadata is not downloaded.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/^\d+$/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/^\d+$/]], 'deltarpm_metadata_percentage'),

    # Username to use for basic authentication to a repo or really any url.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'username'),

    # Password to use with the username for basic authentication.
    # Set this to `absent` to remove it from the file completely.
    # 
    # Valid values are `absent`. Values can match `/.*/`.
    Puppet::Resource::Param(Variant[Enum['absent'], Pattern[/.*/]], 'password')
  ],
  [
    # The name of the repository.  This corresponds to the
    # `repositoryid` parameter in `yum.conf(5)`.
    Puppet::Resource::Param(Any, 'name', true),

    # The target parameter will be enabled in a future release and should not be used.
    Puppet::Resource::Param(Any, 'target'),

    # The specific backend to use for this `yumrepo`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # inifile
    # : Manage yum repo configurations by parsing yum INI configuration files.
    # 
    #   ### Fetching instances
    # 
    #   When fetching repo instances, directory entries in '/etc/yum/repos.d',
    #   '/etc/yum.repos.d', and the directory optionally specified by the reposdir
    #   key in '/etc/yum.conf' will be checked. If a given directory does not exist it
    #   will be ignored. In addition, all sections in '/etc/yum.conf' aside from
    #   'main' will be created as sections.
    # 
    #   ### Storing instances
    # 
    #   When creating a new repository, a new section will be added in the first
    #   yum repo directory that exists. The custom directory specified by the
    #   '/etc/yum.conf' reposdir property is checked first, followed by
    #   '/etc/yum/repos.d', and then '/etc/yum.repos.d'. If none of these exist, the
    #   section will be created in '/etc/yum.conf'.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
