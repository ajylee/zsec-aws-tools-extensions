import attr
import boto3
import zipfile
import uuid
import abc
import toolz
from zsec_aws_tools.aws_lambda import zip_string
from typing import Iterable, Callable, Mapping, Generator, Any, List, Tuple, Union, Dict, Optional

from zsec_aws_tools.basic import AWSResource


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
    if config is None, then
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

    def complete_dependents(self, collection: AWSResourceCollection, element, **kwargs) -> Any:
        if isinstance(element, Mapping):
            completed = {}
            for kk, vv in element.items():
                completed[kk] = self.complete_dependents(collection, vv, **kwargs)
            return completed
        elif isinstance(element, List):
            completed = []
            for sub_elt in element:
                completed.append(self.complete_dependents(collection, sub_elt, **kwargs))
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
            core_kwargs['config'] = self.complete_dependents(collection, element=self.config, **kwargs)

        combined_kwargs = toolz.merge(kwargs, core_kwargs, self.kwargs)

        return self.type_(**combined_kwargs)

    def __hash__(self):
        return hash(self.ztid)

    def partial_attribute(self, name):
        return PartialResourceAttribute(self, name)


class PartialAWSResourceCollection(Iterable):
    _resources: Dict[uuid.UUID, PartialResource]

    def __init__(self):
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

        kwargs = dict(session=session)
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


def get_latest_layer_version(client, LayerName: str):
    """Returns the latest version of an AWS Lambda layer."""
    return max(
        client.list_layer_versions(LayerName=LayerName)['LayerVersions'],
        key=lambda x: x['Version']
    )['LayerVersionArn']
