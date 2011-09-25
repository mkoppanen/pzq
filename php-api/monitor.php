<?php

include 'PZQClient.php';

$m = new PZQMonitor ("ipc:///tmp/pzq-monitor");
$m->get_stats ();