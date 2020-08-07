# This file was automatically generated on 2020-08-03 00:01:47 -0400.
# Use the 'puppet generate types' command to regenerate this file.

# Fails when a previous background installation failed. The type
# will check for the existance of an installation failure log
# and raise an error with the contents of the log if it exists
Puppet::Resource::ResourceType3.new(
  'puppet_agent_upgrade_error',
  [
    # whether or not the error log exists
    Puppet::Resource::Param(Any, 'ensure_notexist')
  ],
  [
    # The name of the failure log to check for in puppet's $statedir. If this log exists the resource will fail.
    Puppet::Resource::Param(Any, 'name', true),

    # The specific backend to use for this `puppet_agent_upgrade_error`
    # resource. You will seldom need to specify this --- Puppet will usually
    # discover the appropriate provider for your platform.Available providers are:
    # 
    # puppet_agent_upgrade_error
    # :
    Puppet::Resource::Param(Any, 'provider')
  ],
  {
    /(?m-ix:(.*))/ => ['name']
  },
  true,
  false)
