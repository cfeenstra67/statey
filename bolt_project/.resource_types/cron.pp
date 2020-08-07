# This file was automatically generated on 2020-08-03 00:01:46 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Installs and manages cron jobs. Every cron resource created by Puppet
# requires a command and at least one periodic attribute (hour, minute,
# month, monthday, weekday, or special). While the name of the cron job is
# not part of the actual job, the name is stored in a comment beginning with
# `# Puppet Name: `. These comments are used to match crontab entries created
# by Puppet with cron resources.
# 
# If an existing crontab entry happens to match the scheduling and command of a
# cron resource that has never been synced, Puppet defers to the existing
# crontab entry and does not create a new entry tagged with the `# Puppet Name: `
# comment.
# 
# Example:
# 
#     cron { 'logrotate':
#       command => '/usr/sbin/logrotate',
#       user    => 'root',
#       hour    => 2,
#       minute  => 0,
#     }
# 
# Note that all periodic attributes can be specified as an array of values:
# 
#     cron { 'logrotate':
#       command => '/usr/sbin/logrotate',
#       user    => 'root',
#       hour    => [2, 4],
#     }
# 
# ...or using ranges or the step syntax `*/2` (although there's no guarantee
# that your `cron` daemon supports these):
# 
#     cron { 'logrotate':
#       command => '/usr/sbin/logrotate',
#       user    => 'root',
#       hour    => ['2-4'],
#       minute  => '*/10',
#     }
# 
# **Important:** _The Cron type will not reset parameters that are
# removed from a manifest_. For example, removing a `minute => 10` parameter
# will not reset the minute component of the associated cronjob to `*`.
# These changes must be expressed by setting the parameter to
# `minute => absent` because Puppet only manages parameters that are out of
# sync with manifest entries.
# 
# **Autorequires:** If Puppet is managing the user account specified by the
# `user` property of a cron resource, then the cron resource will autorequire
# that user.
Puppet::Resource::ResourceType3.new(
  'cron',
  [
    # The basic property that the resource should be in.
    # 
    # Valid values are `present`, `absent`.
    Puppet::Resource::Param(Enum['present', 'absent'], 'ensure'),

    # The command to execute in the cron job.  The environment
    # provided to the command varies by local system rules, and it is
    # best to always provide a fully qualified command.  The user's
    # profile is not sourced when the command is run, so if the
    # user's environment is desired it should be sourced manually.
    # 
    # All cron parameters support `absent` as a value; this will
    # remove any existing values for that field.
    Puppet::Resource::Param(Any, 'command'),

    # A special value such as 'reboot' or 'annually'.
    # Only available on supported systems such as Vixie Cron.
    # Overrides more specific time of day/week settings.
    # Set to 'absent' to make puppet revert to a plain numeric schedule.
    Puppet::Resource::Param(Any, 'special'),

    # The minute at which to run the cron job.
    # Optional; if specified, must be between 0 and 59, inclusive.
    Puppet::Resource::Param(Any, 'minute'),

    # The hour at which to run the cron job. Optional;
    # if specified, must be between 0 and 23, inclusive.
    Puppet::Resource::Param(Any, 'hour'),

    # The weekday on which to run the command. Optional; if specified,
    # must be either:
    # 
    # -   A number between 0 and 7, inclusive, with 0 or 7 being Sunday
    # -   The name of the day, such as 'Tuesday'.
    Puppet::Resource::Param(Any, 'weekday'),

    # The month of the year. Optional; if specified,
    # must be either:
    # 
    # -   A number between 1 and 12, inclusive, with 1 being January
    # -   The name of the month, such as 'December'.
    Puppet::Resource::Param(Any, 'month'),

    # The day of the month on which to run the
    # command.  Optional; if specified, must be between 1 and 31.
    Puppet::Resource::Param(Any, 'monthday'),

    # Any environment settings associated with this cron job.  They
    # will be stored between the header and the job in the crontab.  There
    # can be no guarantees that other, earlier settings will not also
    # affect a given cron job.
    # 
    # 
    # Also, Puppet cannot automatically determine whether an existing,
    # unmanaged environment setting is associated with a given cron
    # job.  If you already have cron jobs with environment settings,
    # then Puppet will keep those settings in the same place in the file,
    # but will not associate them with a specific job.
    # 
    # Settings should be specified exactly as they should appear in
    # the crontab, like `PATH=/bin:/usr/bin:/usr/sbin`.
    Puppet::Resource::Param(Any, 'environment'),

    # The user who owns the cron job.  This user must
    # be allowed to run cron jobs, which is not currently checked by
    # Puppet.
    # 
    # This property defaults to the user running Puppet or `root`.
    # 
    # The default crontab provider executes the system `crontab` using
    # the user account specified by this property.
    Puppet::Resource::Param(Any, 'user'),

    # The name of the crontab file in which the cron job should be stored.
    # 
    # This property defaults to the value of the `user` property if set, the
    # user running Puppet or `root`.
    # 
    # For the default crontab provider, this property is functionally
    # equivalent to the `user` property and should be avoided. In particular,
    # setting both `user` and `target` to different values will result in
    # undefined behavior.
    Puppet::Resource::Param(Any, 'target')
  ],
  [
    # The symbolic name of the cron job.  This name
    # is used for human reference only and is generated automatically
    # for cron jobs found on the system.  This generally won't
    # matter, as Puppet will do its best to match existing cron jobs
    # against specified jobs (and Puppet adds a comment to cron jobs it adds),
    # but it is at least possible that converting from unmanaged jobs to
    # managed jobs might require manual intervention.
    Puppet::Resource::Param(Any, 'name', true),

    # The specific backend to use for this `cron`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # crontab
    # : * Required binaries: `crontab`.
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
