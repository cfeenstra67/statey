
define jupyter::nginx_server(
	$python_version = '3.6',
	$numpy_version = '1.12.1',
	$packages = []
) {

	include jupyter::nginx_amazon_linux

	include nginx

	nginx::resource::server { 'jupyterlab-test':
		listen_port => 80,
		proxy => 'http://localhost:8080',
		proxy_http_version => '1.1',
		proxy_set_header => [
			'Upgrade $http_upgrade',
			'Connection "Upgrade"',
			'Host $host'
		],
		proxy_read_timeout => '86400'
	}

	include anaconda

	anaconda::env { 'notebook':
		python => $python_version,
		numpy => $numpy_version
	}

	jupyter::anaconda_package { 'jupyterlab':
		env => 'notebook',
		install_args => '-c conda-forge'
	}

	include jupyter::daemon_reload

	file { 'jupyter_service_file':
		path => '/etc/systemd/system/jupyter.service',
		source => 'puppet:///modules/jupyter/jupyter.service',
		notify => [
			Class['jupyter::daemon_reload'],
			Service['jupyter']
		]
	}

	file { 'jupyter_home':
		path => '/opt/jupyter',
		ensure => 'directory'
	}

	file { 'jupyter_notebooks_dir':
		path => '/opt/jupyter/notebooks',
		ensure => 'directory',
		require => [File['jupyter_home']]
	}

	file { 'jupyter_config_dir':
		path => '/opt/jupyter/config',
		ensure => 'directory',
		require => [File['jupyter_home']]
	}

	file { 'jupyter_config_file':
		path => '/opt/jupyter/config/config.py',
		source => 'puppet:///modules/jupyter/jupyter_notebook_config.py',
		require => [File['jupyter_config_dir']],
		notify => Service['jupyter']
	}

	file { 'jupyter_env_file':
		path => '/opt/jupyter/config/env',
		source => 'puppet:///modules/jupyter/jupyter-env',
		notify => File['jupyter_service_file']
	}

	service { 'jupyter':
		ensure => running,
		enable => true,
		require => [
			File['jupyter_service_file'],
			File['jupyter_config_file'],
			File['jupyter_notebooks_dir'],
			Class['jupyter::daemon_reload']
		]
	}

  # exec { "install_jupyterlab":
  #     command => "/opt/anaconda/bin/conda install --yes --quiet ${env_option} ${name}",
  #     require => [Anaconda::Env['notebook']],
      
  #     # Ugly way to check if package is already installed
  #     # bug: conda list returns 0 no matter what so we grep output
  #     unless  => "${conda} list ${env_option} ${name} | grep -q -w -i '${name}'",
  # }

}
