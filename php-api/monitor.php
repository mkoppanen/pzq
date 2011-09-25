<?php

include 'PZQClient.php';

$m = new PZQMonitor ("ipc:///tmp/pzq-monitor");
var_dump ($m->get_stats ());