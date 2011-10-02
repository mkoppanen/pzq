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
      --help                                produce help message
      --database arg (=/tmp/sink.kch)       Database sink file location
      --ack-timeout arg (=5000000)          How long to wait for ACK before 
                                            resending message (microseconds)
      --reaper-frequency arg (=2500000)     How often to clean up expired messages 
                                            (microseconds)
      --hard-sync                           If enabled the data is flushed to disk 
                                            on every sync
      --inflight-size arg (=31457280)       Maximum size in bytes for the in-flight
                                            messages database. Full database causes
                                            LRU collection
      --receive-dsn arg (=tcp://*:11131)    The DSN for the receive socket
      --send-dsn arg (=tcp://*:11132)       The DSN for the backend client 
                                            communication socket
      --monitor-dsn arg (=ipc:///tmp/pzq-monitor)
                                            The DSN for the monitoring socket


Consistency
===========

--ack-timeout
Defines how long to wait for an ACK for message delivered before scheduling
it for retransmission.

--hard-sync
Define this option for physical synchronization with the device, or leave out
for logical synchronization with the file system.

--inflight-size
Defines the maximum size in bytes for the messages that are in flight. Setting
this database small can harm performance as LRU needs to run more often and 
the messages that were in flight need to be retransmitted.

Centos Notes
======

Older versions will require the boost141-devel package, available from EPEL. 

Specify the location by building with 

    $ cmake .. -DBOOST_INCLUDEDIR=/usr/include/boost141 -DBOOST_LIBRARYDIR=/usr/lib64/boost141



TODO
====

- Add tests
