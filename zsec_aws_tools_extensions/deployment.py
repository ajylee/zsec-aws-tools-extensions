import logging
import textwrap
from functools import partial
from operator import getitem, itemgetter
from types import MappingProxyType

import attr
import boto3
import zipfile
import uuid
import abc
import toolz
from toolz import curried
from zsec_aws_tools.aws_lambda import zip_string, FunctionResource
import zsec_aws_tools.iam as zaws_iam
from typing import Iterable, Callable, Mapping, Generator, Any, List, Tuple, Union, Dict, Optional

from zsec_aws_tools.basic import AWSResource, get_account_id
from .session_management import SessionSource

logger = logging.getLogger(__name__)


def get_zrn(account_number: str, region_name: str, ztid: uuid.UUID):
    return f'zrn:aws:{account_number}:{region_name}:{str(ztid).lower()}'


def get_resource_meta_description(res) -> Dict[str, str]:
    if isinstance(res, AWSResource):
        account_number = get_account_id(res.session)
        zrn = get_zrn(account_number, res.region_name, res.ztid)
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


class AWSResourceCollection(Iterable):
    def __init__(self):
        self._resources = {}

    def append(self, resource: AWSResource):
        self._resources[resource.ztid] = resource

    def extend(self, resources: Iterable[AWSResource]):
        for resource in resources:
            self.append(resource)

    def __iter__(self):
        yield from self._resources.values()

    def __getitem__(self, key):
        return self._resources[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key, value):
        self._resources[key] = value


class GenericResource:
    def __init__(self, ztid, name, fn):
        self.ztid = ztid
        self.name = name
        self.fn = fn
        self.exists = False

    def put(self):
        self.fn()

    def delete(self):
        pass


CompleteResource = Union[GenericResource, AWSResource]


class PartialResourceABC(abc.ABC):
    ztid: uuid.UUID

    def complete(self, collection: 'AWSResourceCollection', **kwargs) -> CompleteResource:
        ...


@attr.s(auto_attribs=True)
class PartialGenericResource(PartialResourceABC):
    ztid: uuid.UUID
    name: str
    fn: Callable
    args: Iterable[PartialResourceABC]
    kwargs: Mapping[Any, PartialResourceABC]

    def complete(self, collection: 'AWSResourceCollection', **kwargs) -> CompleteResource:
        completed_args = (arg.complete(collection, **kwargs) for arg in self.args)
        completed_kwargs = {kk: vv.complete(collection, **kwargs) for kk, vv in self.kwargs.items()}

        def thunk():
            return self.fn(*completed_args, **completed_kwargs)

        return GenericResource(self.ztid, self.name, thunk)


def partial_resources(ztid, *args: PartialResourceABC, **kwargs):
    def _inner1(fn):
        name = fn.__name__
        return PartialGenericResource(ztid, name, fn, args, kwargs)

    return _inner1


class PartialResourceAttribute:
    parent: 'PartialResource'
    name: str

    def __init__(self, parent: 'PartialResource', name: str):
        self.parent = parent
        self.name = name

    def complete(self, collection: 'AWSResourceCollection', **kwargs) -> Any:
        if self.parent.ztid not in collection:
            collection[self.parent.ztid] = self.parent.complete(collection, **kwargs)

        # the lambda is to make sure it only gets evaluated when processing config.
        return lambda _: getattr(collection[self.parent.ztid], self.name)


class PartialResource(PartialResourceABC):
    """
    Partially Defined Resource

    Note that a PartialResource could be thought of as a template. However, they are not called ResourceTemplate
    because template may connote that the code is meant for broad reuse, whereas PartialResources can be quite specific.

    Note: if config is None, then it will be assumed to already exist, so there will be no completion of dependencies.

    """
    ztid: uuid.UUID
    type_: type

    def __init__(self, type_: type, config: Mapping = None, name: str = None,
                 index_id: str = None, ztid: uuid.UUID = None, **kwargs):
        self.type_ = type_
        self.name = name
        self.ztid = ztid
        self.kwargs = kwargs
        self.config = config
        self.index_id = index_id

    def complete_dependencies(self, collection: AWSResourceCollection, element, **kwargs) -> Any:
        if isinstance(element, Mapping):
            completed = {}
            for kk, vv in element.items():
                completed[kk] = self.complete_dependencies(collection, vv, **kwargs)
            return completed
        elif isinstance(element, List):
            completed = []
            for sub_elt in element:
                completed.append(self.complete_dependencies(collection, sub_elt, **kwargs))
            return completed
        elif isinstance(element, PartialResource):
            if element.ztid not in collection:
                collection[element.ztid] = element.complete(collection, **kwargs)
            return collection[element.ztid]
        elif isinstance(element, PartialResourceAttribute):
            return element.complete(collection, **kwargs)
        else:
            return element

    def complete(self, collection: AWSResourceCollection, **kwargs) -> AWSResource:
        core_kwargs: Dict[str, Any]
        core_kwargs = dict(name=self.name, ztid=self.ztid)
        if self.config is not None:
            core_kwargs['config'] = self.complete_dependencies(collection, element=self.config, **kwargs)

        combined_kwargs = toolz.merge(kwargs, core_kwargs, self.kwargs)

        return self.type_(**combined_kwargs)

    def __hash__(self):
        return hash(self.ztid)

    def partial_attribute(self, name):
        return PartialResourceAttribute(self, name)

    def partial_fill(self, override=True, **kwargs) -> __qualname__:
        core_kwargs: Dict[str, Any]
        core_kwargs = dict(name=self.name, ztid=self.ztid, config=self.config)
        if override:
            combined_kwargs = toolz.merge(core_kwargs, self.kwargs, kwargs)
        else:
            combined_kwargs = toolz.merge(kwargs, core_kwargs, self.kwargs)
        return __class__(**combined_kwargs)


class PartialResourceCollection(Iterable):
    """
    Partially Defined Resource Collection

    A collection of PartialResources. When the `complete` method of the collection is called with keyword arguments,
    the `complete` method of each element of the collection is called with the same keyword arguments passed through.

    Additionally, you can pass keyword arguments when calling the constructor. These keyword arguments will
    also be passed to the `complete` method of each element when the `PartialResourceCollection.complete` is called.

    """
    _resources: Dict[uuid.UUID, PartialResource]

    def __init__(self, **kwargs):
        self.collection_common_kwargs = kwargs
        self._resources = {}

    def new_partial_resource(self, type_: type, config: Mapping, name: str = None,
                             index_id: str = None, ztid: uuid.UUID = None, **kwargs) -> PartialResource:
        resource = PartialResource(type_=type_, config=config, name=name,
                                   index_id=index_id, ztid=ztid, **kwargs)
        self.append(resource)
        return resource

    def append(self, resource: PartialResource):
        assert resource.ztid not in self._resources
        self._resources[resource.ztid] = resource

    def extend(self, resources: Iterable[PartialResource]):
        for resource in resources:
            self.append(resource)

    def __iter__(self) -> Generator[PartialResource, None, None]:
        yield from self._resources.values()

    def __getitem__(self, key):
        return self._resources[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def complete(self, session: boto3.Session, region_name: str = None, manager: str = None) -> AWSResourceCollection:
        completed_collection = AWSResourceCollection()

        kwargs = dict(session=session, **self.collection_common_kwargs)
        if region_name:
            kwargs['region_name'] = region_name
        if manager:
            kwargs['manager'] = manager

        for partial_resource in self:
            if partial_resource.ztid not in completed_collection:
                if isinstance(partial_resource, (PartialResource, PartialGenericResource)):
                    completed_collection[partial_resource.ztid] = partial_resource.complete(
                        completed_collection, **kwargs)

        return completed_collection


PartialAWSResourceCollection = PartialResourceCollection


def get_latest_layer_version(client, LayerName: str):
    """Returns the latest version of an AWS Lambda layer."""
    return max(
        client.list_layer_versions(LayerName=LayerName)['LayerVersions'],
        key=lambda x: x['Version']
    )['LayerVersionArn']


def deserialize_resource(session, region_name, type: str, ztid, index_id):
    import importlib

    module_name = '.'.join(type.split('.')[:-1])
    leaf_name = type.split('.')[-1]

    module = importlib.import_module(module_name)

    _type = getattr(module, leaf_name)

    return _type(session=session, region_name=region_name, ztid=ztid, index_id=index_id)


def collect_garbage(recorder, scope, deployment_id, max_marked_dependency_order, dry):
    _unmarked = partial(recorder.unmarked, scope=scope, deployment_id=deployment_id)

    logger.info('collecting garbage{}'.format(' (dry)' if dry else ''))

    if dry:
        delta = None
        for dependency_order, zrn, resource in _unmarked(high_to_low_dependency_order=False):
            print(f'would delete: {resource.name}(ztid={resource.ztid}) : {type(resource).__name__}')
            print('updating dependency_orders')
            if delta is None:
                delta = max_marked_dependency_order + 1 - dependency_order
            recorder.update_dependency_order(zrn, dependency_order + delta)
    else:
        for dependency_order, zrn, resource in _unmarked(high_to_low_dependency_order=True):
            print(f'deleting: {resource.name}(ztid={resource.ztid}) : {type(resource).__name__}')
            delete_by_zrn(recorder, zrn, resource)


def delete_by_zrn(recorder: 'ResourceRecorder', zrn: str, resource: AWSResource):
    # TODO: combine with `delete_resource_nice`
    if isinstance(resource, zaws_iam.Role):
        print('detaching policies')
        resource.detach_all_policies()
        print('deleting inline policies')
        resource.delete_inline_policies()

    resource.delete(not_exists_ok=True)
    recorder.delete_record_by_zrn(zrn)


class ResourceRecorder(abc.ABC):
    manager: str

    @abc.abstractmethod
    def put_resource_record(self, manager, deployment_id: uuid.UUID, dependency_order: int, resource: AWSResource):
        ...

    @abc.abstractmethod
    def delete_resource_record(self, manager, resource: AWSResource):
        ...

    @abc.abstractmethod
    def update_dependency_order(self, dependency_order, resource: AWSResource):
        ...

    @abc.abstractmethod
    def unmarked(self, scope: Mapping[str, str], deployment_id, high_to_low_dependency_order: bool
                 ) -> Iterable[AWSResource]:
        ...

    @abc.abstractmethod
    def delete_record_by_zrn(self, zrn):
        ...


class DynamoResourceRecorder(ResourceRecorder):
    def __init__(self, resources_by_zrn_table, session_source: SessionSource):
        self.resources_by_zrn_table = resources_by_zrn_table
        self.session_source = session_source
        super().__init__()

    def put_resource_record(self, manager, deployment_id: uuid.UUID, dependency_order: int, resource: AWSResource):
        item = toolz.merge(
            get_resource_meta_description(resource),
            dict(deployment_id=str(deployment_id).lower(),
                 manager=manager,
                 dependency_order=dependency_order))
        self.resources_by_zrn_table.put_item(**item)

    def update_dependency_order(self, dependency_order, resource: AWSResource):
        zrn = get_zrn(get_account_id(resource.session), resource.region_name, resource.ztid)
        self.resources_by_zrn_table.update_item(
            Key={'zrn': zrn}, AttributeUpdates={'dependency_order': {'Value': dependency_order, 'Action': 'PUT'}},
        )

    def delete_resource_record(self, manager, resource: AWSResource):
        zrn = get_zrn(get_account_id(resource.session), resource.region_name, resource.ztid)
        self.resources_by_zrn_table.delete_item(Key={'zrn': zrn})

    def unmarked(self, scope: Mapping[str, str], deployment_id,
                 high_to_low_dependency_order: bool) -> Iterable[AWSResource]:
        from boto3.dynamodb.conditions import Key, Attr

        # TODO: use GSI query on manager
        filter_expression = ~Attr('deployment_id').eq(str(deployment_id).lower())
        for kk, vv in scope.items():
            filter_expression &= Attr(kk).eq(vv)

        response = self.resources_by_zrn_table.scan(FilterExpression=filter_expression, ConsistentRead=True)

        assert 'LastEvaluatedKey' not in response, textwrap.wrap(textwrap.dedent('''needs pagination, violating behavior 
            specified in documentation at 
            https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
        '''))

        for item in sorted(response['Items'], key=itemgetter('dependency_order'), reverse=high_to_low_dependency_order):
            session = self.session_source.get_session(item['account_number'])
            resource = deserialize_resource(session, item['region_name'], item['type'], item['ztid'], item['index_id'])
            zrn = item['zrn']
            yield item['dependency_order'], zrn, resource

    def delete_record_by_zrn(self, zrn):
        self.resources_by_zrn_table.delete_item(Key=dict(zrn=zrn))


class LambdaResourceRecorder(ResourceRecorder, abc.ABC):
    def __init__(self, put_resource_record_lambda: FunctionResource, delete_resource_record_lambda: FunctionResource):
        self.put_resource_record_lambda = put_resource_record_lambda
        self.delete_resource_record_lambda = delete_resource_record_lambda
        ResourceRecorder.__init__(self)

    def put_resource_record(self, manager, deployment_id: uuid.UUID, dependency_order: int,
                            resource: AWSResource):
        if self.put_resource_record_lambda and self.put_resource_record_lambda.exists and resource.exists:
            payload = toolz.merge(
                get_resource_meta_description(resource),
                dict(deployment_id=str(deployment_id).lower(),
                     manager=manager,
                     dependency_order=dependency_order))
            resp = self.put_resource_record_lambda.invoke(json_codec=True, Payload=payload)

            if resp:
                print(resp)
        else:
            print('put resource record failed')

    def delete_resource_record(self, manager, resource: AWSResource):
        if self.delete_resource_record and self.delete_resource_record_lambda.exists:
            resp = self.delete_resource_record_lambda.invoke(
                json_codec=True,
                Payload=toolz.assoc(get_resource_meta_description(resource),
                                    'manager', manager))
            if resp:
                print(resp)
        else:
            print('delete resource record failed')


class MixedLambdaDynamoResourceRecorder(LambdaResourceRecorder, DynamoResourceRecorder, ResourceRecorder):
    def __init__(
            self,
            put_resource_record_lambda: FunctionResource,
            resources_by_zrn_table,
            session_source: SessionSource
    ):
        LambdaResourceRecorder.__init__(self, put_resource_record_lambda, None)
        DynamoResourceRecorder.__init__(self, resources_by_zrn_table, session_source=session_source)
        ResourceRecorder.__init__(self)

    def delete_resource_record(self, manager, resource):
        return DynamoResourceRecorder.delete_resource_record(self, manager, resource)
