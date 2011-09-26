pzq
===

A very simple store and forward device using ZeroMQ.

Building
========

    $ mkdir build
    $ cd build
    $ cmake .. -DZEROMQ_ROOT=/path/to -DKYOTOCABINET_ROOT=/path/to
    $ make

Options
=======

        Command-line options:
          --help                             produce help message
          --database arg (=/tmp/sink.kch)    Database sink file location
          --ack-timeout arg (=5000000)       How long to wait for ACK before resending 
                                             message (microseconds)
          --sync-divisor arg (=0)            The divisor for sync to the disk. 0 causes
                                             sync after every message
          --hard-sync                        If enabled the data is flushed to disk on 
                                             every sync
          --inflight-size arg (=31457280)    Maximum size in bytes for the in-flight 
                                             messages database. Full database causes 
                                             LRU collection
          --receive-dsn arg (=tcp://*:11131) The DSN for the receive socket
          --send-dsn arg (=tcp://*:11132)    The DSN for the backend client 
                                             communication socket
          --monitor-dsn arg (=tcp://*:11132) The DSN for the monitoring socket


Consistency
===========

--ack-timeout
Defines how long to wait for an ACK for message delivered before scheduling
it for retransmission.

--sync-divisor
Sync divisor is used to determine how often the messages should be flushed
to disk. On each operation a random number between 0 and sync_divisor is chosen and if the number is 0 then a sync is done. Setting  higher sync
divisor causes things to run faster but this comes at the cost of possibly 
losing more messages on crash.

--hard-sync
Define this option for physical synchronization with the device, or leave out
for logical synchronization with the file system.

--inflight-size
Defines the maximum size in bytes for the messages that are in flight. Setting
this database small can harm performance as LRU needs to run more often and 
the messages that were in flight need to be retransmitted.


TODO
====

- Add tests