Plans
======


Testing
-----------

 - implement mocking IO monad that would reorder IO actions, if that's possible.
 - implement simulation of crashes on file close a-la SQLite.
 - implement testing of durability despite crashes.


Storage layer
--------------

 - implement compressed runs - keys and data between references should be compressed.


Durability
------------

 - implement sliced runs with redundancy information. Sliced run is a run where data is sliced between
   different blocks of run. E.g., first 16 bytes go to first block, second 15 bytes go to second,
   third 16 bytes to third, etc. There's a block with redundancy info, computes some codes to recover
   from error(s).
