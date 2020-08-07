
class jupyter::daemon_reload {

  exec { '/usr/bin/systemctl daemon-reload':
    refreshonly => true,
  }

}
