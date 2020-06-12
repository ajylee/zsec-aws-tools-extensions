import argparse
from types import MappingProxyType
from typing import Dict, Optional, Iterable, Mapping, Tuple
from toolz import assoc, merge, memoize
import uuid

from zsec_aws_tools.basic import AWSResource, get_account_id
from zsec_aws_tools.aws_lambda import FunctionResource
import zsec_aws_tools.iam as zaws_iam

import logging

from .deployment import collect_garbage, ResourceRecorder, get_resource_meta_description, get_zrn

logger = logging.getLogger(__name__)


def put_resource_nice(
        manager,
        resource: AWSResource,
        dependency_order: int,
        force: bool,
        recorder: Optional[ResourceRecorder],
        deployment_id: uuid.UUID,
):
    """

    :param recorder:
    :param manager:
    :param resource:
    :param dependency_order: within a memory management scope, resources of higher dependency_order can only depend on
        resources of lower dependency_order.
    :param force:
    :param put_resource_record:
    :param deployment_id:
    :return:
    """
    if resource.config:
        print(f'applying: {resource.name}(ztid={resource.ztid}) : {type(resource).__name__}')
        resource.put(force=force, wait=True)
        if resource.exists and recorder:
            recorder.put_resource_record(manager, deployment_id, dependency_order, resource)


def delete_resource_nice(
        manager,
        resource: AWSResource,
        force: bool,
        recorder: ResourceRecorder,
):
    if force:
        raise NotImplementedError('Need to implement manager check for delete.')

    if resource.exists:
        if isinstance(resource, zaws_iam.Role):
            print('detaching policies')
            resource.detach_all_policies()
        print('deleting: ', resource)
        resource.delete()
        if not resource.exists and recorder:
            recorder.delete_resource_record(manager, resource)
    else:
        print('does not exist: ', resource)


def handle_cli_command(
        manager: str,
        resources: Iterable[AWSResource],
        support_gc: bool = False,
        gc_scope: Mapping[str, str] = None,
        recorder: ResourceRecorder = None,
):
    """

    :param manager: Used for "memory management" for resources.
    :param resources: Resources to put.
    :param support_gc: Whether to support garbage collection.
    :param gc_scope: defines a filter on attributes of resources in order to be considered in-scope for this deployment.
        This limits the garbage collection scope.
        E.g. `{'manager': manager, 'account_number': '123456789000'}`
        limit the GC scope to only resources with the specified manager and in the specified account.
        Default is `None`. If scope is `None`, this function behaves as if scope were set to `{'manager': manager}`.
    :param recorder: used for recording resource deployment state
    :return:
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subparser_name')
    apply_parser = subparsers.add_parser('apply')
    apply_parser.add_argument('--force', '-f', action='store_true',
                              help='take ownership and apply resource configs even if not initially owned')
    destroy_parser = subparsers.add_parser('destroy')
    destroy_parser.add_argument('--force', '-f', action='store_true',
                                help='destroy resources even if not owned')

    parser.add_argument('--verbose', '-v', action='store_true', help='increase log level')

    for subparser in (apply_parser, destroy_parser):
        subparser.add_argument('--only-ztids', nargs='+', action='extend', type=uuid.UUID,
                               help='Only apply/destroy resources with particular ztids. May affect depedencies and'
                                    'dependents. If specified, there will be no garbage collection.')

        subparser.add_argument('--deployment-id', nargs=1, action='extend', type=uuid.UUID,
                               help='deployment id for mark and sweep garbage collection')

        subparser.add_argument('--dry-gc', action='store_true',
                               help='do not garbage collect, only report. If --only-ztids` is specified, this flag '
                                    'is redundant because GC will be skipped.')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    force = args.subparser_name in ('apply', 'destroy') and args.force

    want_gc = args.subparser_name in ('apply', 'destroy') and support_gc and not args.only_ztids

    resource: AWSResource
    deployment_id = args.deployment_id or uuid.uuid4()
    nn = 0

    _get_account_id = memoize(get_account_id)

    if args.subparser_name == 'apply' or (args.subparser_name is None):
        applied = set()
        for nn, resource in enumerate(resources):
            if not args.only_ztids or resource.ztid in args.only_ztids:
                zrn = get_zrn(
                    account_number=_get_account_id(resource.session),
                    region_name=resource.region_name,
                    ztid=resource.ztid,
                    clouducer_path=resource.clouducer_path,
                )
                if zrn not in applied:
                    put_resource_nice(
                        manager, resource,
                        dependency_order=nn,
                        force=force,
                        recorder=recorder,
                        deployment_id=deployment_id,
                    )
                    applied.add(zrn)

    elif args.subparser_name == 'destroy':
        destroyed = set()
        for resource in resources:
            if not args.only_ztids or resource.ztid in args.only_ztids:
                zrn = get_zrn(
                    account_number=_get_account_id(resource.session),
                    region_name=resource.region_name,
                    ztid=resource.ztid,
                    clouducer_path=resource.clouducer_path,
                )
                if zrn not in destroyed:
                    delete_resource_nice(manager, resource, force=force, recorder=recorder)
                    destroyed.add(zrn)

    max_marked_dependency_order = nn

    if support_gc:
        assert manager and recorder

        if want_gc:
            if gc_scope is None:
                gc_scope = {'manager': manager}

            if 'manager' not in gc_scope:
                raise ValueError("GC scope without manager is too dangerous")

            collect_garbage(recorder, gc_scope, deployment_id, max_marked_dependency_order, args.dry_gc)
        else:
            print('no gc')
    else:
        print('gc not supported, skipping')
