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

Centos Notes
======

Older versions will require the boost141-devel package, available from EPEL. 

Specify the location by building with 

    $ cmake .. -DBOOST_INCLUDEDIR=/usr/include/boost141 -DBOOST_LIBRARYDIR=/usr/lib64/boost141

Communication
=============

In general, anything before following the empty message
part should be considered protocol data. The client 
should assume that in future more header information can
be added to the messages.

- Producing a message

    +--------------------+
    | message id         |
    +--------------------+
    | 0 size part        |
    +--------------------+
    | 1..N message parts |
    +--------------------+

- Producer ACK message

    +---------------------+
    | message id          |
    +---------------------+
    | status code 0/1     |
    +---------------------+
    | 0 size part         |
    +---------------------+
    | 0..1 status message |
    +---------------------+

*Note*: Status code 1 for success and 0 for failure. 
        Status message is set in case of failure.

- Consumer message

    +---------------------+
    | peer id             |
    +---------------------+
    | message id          |
    +---------------------+
    | sent time           |
    +---------------------+
    | ack timeout         |
    +---------------------+
    | 0 size part         |
    +---------------------+
    | 1..N message parts  |
    +---------------------+
    
- Consumer ACK message

    +---------------------+
    | peer id             |
    +---------------------+
    | message id          |
    +---------------------+
    | status code 0/1     |
    +---------------------+      

*Note*: Status code 1 for success and 0 for failure. 
            


TODO
====

- Add tests
