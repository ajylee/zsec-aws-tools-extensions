import argparse
from typing import Dict, Optional, Iterable
from toolz import assoc
import uuid

from zsec_aws_tools.basic import AWSResource, get_account_id
from zsec_aws_tools.aws_lambda import FunctionResource
import zsec_aws_tools.iam as zaws_iam


def get_resource_meta_description(res) -> Dict[str, str]:
    if isinstance(res, AWSResource):
        account_number = get_account_id(res.session)
        zrn = f'zrn:aws:{account_number}:{res.region_name}:{str(res.ztid).lower()}'
        return dict(
            zrn=zrn,
            account_number=account_number,
            region_name=res.region_name,
            ztid=str(res.ztid),
            name=res.name,
            index_id=res.index_id,
            type='{}.{}'.format(type(res).__module__, type(res).__name__),
        )
    else:
        raise NotImplementedError


def put_resource_nice(
        manager,
        resource: AWSResource,
        force: bool,
        put_resource_record: Optional[FunctionResource]
):
    if resource.config:
        print(f'applying: {resource.name}(ztid={resource.ztid}) : {type(resource).__name__}')
        resource.put(force=force)
        if put_resource_record and put_resource_record.exists and resource.exists:
            resp = put_resource_record.invoke(json_codec=True,
                                              Payload=assoc(get_resource_meta_description(resource),
                                                            'manager', manager))
            if resp:
                print(resp)


def delete_resource_nice(
        manager,
        resource: AWSResource,
        force: bool,
        delete_resource_record: Optional[FunctionResource]
):
    if force:
        raise NotImplementedError('Need to implement manager check for delete.')

    if resource.exists:
        if isinstance(resource, zaws_iam.Role):
            print('detaching policies')
            resource.detach_all_policies()
        print('deleting: ', resource)
        resource.delete()

        if delete_resource_record and delete_resource_record.exists and not resource.exists:
            resp = delete_resource_record.invoke(json_codec=True,
                                                 Payload=assoc(get_resource_meta_description(resource),
                                                               'manager', manager))
            if resp:
                print(resp)
    else:
        print('does not exist: ', resource)


def handle_cli_command(
        manager: str,
        resources: Iterable[AWSResource],
        put_resource_record: Optional[FunctionResource] = None,
        delete_resource_record: Optional[FunctionResource] = None,
):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subparser_name')
    apply_parser = subparsers.add_parser('apply')
    apply_parser.add_argument('--force', '-f', action='store_true',
                              help='take ownership and apply resource configs even if not initially owned')
    destroy_parser = subparsers.add_parser('destroy')
    destroy_parser.add_argument('--force', '-f', action='store_true',
                                help='destroy resources even if not owned')

    for subparser in (apply_parser, destroy_parser):
        subparser.add_argument('--only-ztids', nargs='+', action='extend', type=uuid.UUID,
                               help='Only apply/destroy resources with particular ztids. May affect depedencies and'
                                    'dependents.')

    args = parser.parse_args()
    force = args.subparser_name in ('apply', 'destroy') and args.force

    resource: AWSResource
    if args.subparser_name == 'apply' or (args.subparser_name is None):
        for resource in resources:
            if not args.only_ztids or resource.ztid in args.only_ztids:
                put_resource_nice(manager, resource, force=force, put_resource_record=put_resource_record)

    elif args.subparser_name == 'destroy':
        for resource in resources:
            if not args.only_ztids or resource.ztid in args.only_ztids:
                delete_resource_nice(manager, resource, force=force, delete_resource_record=delete_resource_record)
