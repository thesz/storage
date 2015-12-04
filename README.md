Storage engine, LSM-style
=========================


Generral information
--------------------

This repository contains storage engine that implements persistent key-value storage
in log-structured merge tree (LSM) style. Persistence here means that data once fixed
in transaction commit will not change in future.


Copyright information
---------------------

Copyright (C) 2015 Serguey Zefirov


Source files
-----

S.hs - storage engine,
TS.hs - tests.
