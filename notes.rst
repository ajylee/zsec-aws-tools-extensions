==============================
Destruction-dependent updates
==============================

Example
--------

Changing the name of a Lambda Function requires destroying it. This could be
handled by optimistically calling :code:`put`, then checking if the config is
what is desired. Then the resource needs to be destroyed before calling
:code:`put` again.

There should be an option controlling whether to allow destruction-dependent updates,
and to detect destruction-dependent updates running apply.


===================
Garbage collection
===================

- mark and sweep resources inside of a living module
- mark and sweep entire dead modules
