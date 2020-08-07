
class jupyter::nginx_amazon_linux($package_name = 'nginx1') {

	if $facts['os']['name'] == 'Amazon' {
		exec { "/usr/bin/amazon-linux-extras install -y ${package_name}":
			onlyif => "/usr/bin/amazon-linux-extras list | grep enabled | grep ${package_name} && false || true"
		}
	}	

}
