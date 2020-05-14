===================================================
Garbage Collection Specification and Implementation
===================================================

High Level View
================

Clouducer uses a mark and sweep strategy. Marking happens during
put operations. Each round of deployment has a unique `deployment_id`,
and each resource that is put during the deployment is marked with that `deployment_id`
in the resource database. When all put operations are finished, the resources
under the current GC scope (see below) that are not marked with the current `deployment_id` are
deleted. The order of deletion is the reverse of dependency ordering, so that dependent resources
are deleted before their dependencies.


Garbage Collection Context
==========================

The GC context is determined by the mapping `gc_scope`.
It defines the scope of resources under management by the current deployment.
A resource is in scope if and only if it matches the attributes defined in `gc_scope`.
The default behavior is for `gc_scope` to be `{'manager': manager}`, where the value of `manager` is
defined by the module.


Dependency Ordering
===================

Each resource is assigned an integral `dependency_order`.
For any two resources `A` and `B` where `A < B` when ordering by `dependency_order`,
this means that A cannot depend on B, but B can depend on A. (Note that B does not have to depend on A.)

In practice, the `dependency_order` is just the order in which the resource was deployed, with some
extra provisions for when GC is turned off via the `dry_gc` command line switch, so that
a valid ordering is maintained for the next time GC is enabled.
Each time the module is redeployed, or "trued up", if GC is disabled, then the resources that would
have been deleted instead have their `dependency_order` modified so that for any marked `A`,
unmarked `B`, and unmarked `C`, we maintain the original ordering between `B` and `C`, while requiring
`A < B`, `A < C`. In practice this means the `dependency_order` for unmarked resources is increased by
the number of marked resources minus the least `dependency_order` of the unmarked resources.


Undefined Behavior
==================

When turning on `support_gc`, the behavior is undefined for unmarked resources at the end of the marking/putting
phase. An attempt will be made to collect, but it may fail for nontrivial dependency structures.
Ensure that when turning on `support_gc` for the first time, no unmarked resources will exist during the sweep
phase.

