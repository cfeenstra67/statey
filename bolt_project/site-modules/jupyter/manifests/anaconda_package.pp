# Copied from anaconda module
define jupyter::anaconda_package( $env=undef, $base_path='/opt/anaconda', $install_args='') {
    include anaconda
    
    $conda = "${base_path}/bin/conda"

    # Need environment option if env is set
    # Also requirement on the env being defined
    if $env {
        $env_option = "--name ${env}"
        $env_require = [Class["anaconda::install"], Anaconda::Env[$env] ]
        $env_name = "${env}"
    } else {
        $env_option = ''
        $env_name = "root"
        $env_require = [Class["anaconda::install"]]
    }
    
    
    exec { "anaconda_${env_name}_${name}":
        command => "${conda} install --yes --quiet ${env_option} ${install_args} ${name}",
        require => $env_require,
        
        # Ugly way to check if package is already installed
        # bug: conda list returns 0 no matter what so we grep output
        unless  => "${conda} list ${env_option} ${name} | grep -q -w -i '${name}'",
    }
}
