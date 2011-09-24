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
          --ack-timeout arg (=5)             How long to wait for ACK before resending 
                                             message (seconds)
          --sync-divisor arg (=0)            The divisor for sync to the disk. 0 causes
                                             sync after every message
          --hard-sync                        If enabled the data is flushed to disk on 
                                             every sync
          --inflight-size arg (=31457280)    Maximum size in bytes for the in-flight 
                                             messages database. Full database causes 
                                             LRU collection
          --receive-dsn arg (=tcp://*:11131) The DSN for the receive socket
          --ack-dsn arg (=tcp://*:11132)     The DSN for the ACK socket
          --publish-dsn arg (=tcp://*:11133) The DSN for the backend client 
                                             communication socket
          --use-pubsub                       Changes the backend client communication 
                                             socket to use publish subscribe pattern

TODO
====

- Add tests