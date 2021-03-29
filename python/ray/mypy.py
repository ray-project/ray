from functools import partial
from typing import List, Optional

from mypy.nodes import (ARG_POS, MDEF, Argument, Block, ClassDef, FuncDef,
                        PassStmt, SymbolTableNode, TypeInfo, Var)
from mypy.plugin import (AttributeContext, ClassDefContext, Plugin,
                         SemanticAnalyzerPluginInterface)
from mypy.semanal import set_callable_name
from mypy.types import CallableType, Instance, Type, TypeType, UnionType
from mypy.typevars import fill_typevars


def add_method_to_class(api: SemanticAnalyzerPluginInterface,
                        cls: ClassDef,
                        name: str,
                        args: List[Argument],
                        return_type: Type,
                        self_type: Optional[Type] = None,
                        is_static_method: bool = False) -> None:
    """Adds a new method to a class definition.

    Source: modified from mypy's plugin.common
    """
    info = cls.info

    # First remove any previously generated methods with the same name
    # to avoid clashes and problems in the semantic analyzer.
    if name in info.names:
        sym = info.names[name]
        if sym.plugin_generated and isinstance(sym.node, FuncDef):
            cls.defs.body.remove(sym.node)

    self_type = self_type or fill_typevars(info)
    function_type = api.named_type("__builtins__.function")

    if not is_static_method:
        args = [Argument(Var("self"), self_type, None, ARG_POS)] + args
    arg_types, arg_names, arg_kinds = [], [], []
    for arg in args:
        assert arg.type_annotation, "All arguments must be fully typed."
        arg_types.append(arg.type_annotation)
        arg_names.append(arg.variable.name)
        arg_kinds.append(arg.kind)

    signature = CallableType(arg_types, arg_kinds, arg_names, return_type,
                             function_type)
    func = FuncDef(name, args, Block([PassStmt()]))
    func.info = info
    func.type = set_callable_name(signature, func)
    func._fullname = info.fullname + "." + name
    func.line = info.line
    func.is_static = is_static_method

    info.names[name] = SymbolTableNode(MDEF, func, plugin_generated=True)
    info.defn.defs.body.append(func)


def plugin(version: str):
    return RayActorPlugin


def add_remote_method_to_class(ctx: ClassDefContext):
    actor_handle_def = ctx.api.lookup_fully_qualified_or_none(
        "ray.actor.ActorHandle")
    if actor_handle_def is None:
        return ctx.api.defer()
    actor_handle_type_info = actor_handle_def.node
    assert isinstance(actor_handle_type_info, TypeInfo)

    # TODO: Inject signature from __init__ method
    info = ctx.cls.info
    method_names = [f.name for f in info.defn.defs.body]
    assert "__init__" not in method_names, NotImplementedError()

    add_method_to_class(
        ctx.api,
        ctx.cls,
        "remote",
        args=[],
        return_type=Instance(
            actor_handle_type_info,
            args=[TypeType(Instance(ctx.cls.info, args=[]))]),
        is_static_method=True)


def generate_actor_method_type(ctx: AttributeContext, plugin_obj: Plugin):
    actor_method_name = ctx.context.name
    # Retrieve the type argument from actor handle
    # ActorHandle[Type[UserClass]] -> UserClass
    original_cls = ctx.type.args[0].item.type
    original_method: SymbolTableNode = original_cls.names.get(
        actor_method_name)
    if original_method is None:
        ctx.api.fail(
            f'Actor "{original_cls.name}" has no method "{actor_method_name}"',
            ctx.context)
        return ctx.default_attr_type
    original_func_def: FuncDef = original_method.node

    ray_method_type = Instance(
        plugin_obj.lookup_fully_qualified("ray.actor.ActorMethod").node,
        args=[],
    )

    # Wrap the return type with ObjectRef[T]
    remote_return_type = original_func_def.type.ret_type
    if remote_return_type.type.name != "ObjectRef":
        remote_return_type = Instance(
            plugin_obj.lookup_fully_qualified("ray._raylet.ObjectRef").node,
            args=[remote_return_type])

    # Generate the function def for the handle.method.remote(...)
    remote_node = original_method.copy()
    remote_signature = remote_node.node
    remote_signature.arguments[0] = Argument(
        Var("self"), ray_method_type, None, ARG_POS)
    unioned_arg_types = [
        UnionType([
            arg_type,
            Instance(
                plugin_obj.lookup_fully_qualified("ray._raylet.ObjectRef")
                .node,
                args=[arg_type])
        ]) for arg_type in original_func_def.type.arg_types[1:]
    ]
    remote_signature.type = original_func_def.type.copy_modified(
        arg_types=[ray_method_type] + unioned_arg_types,
        ret_type=remote_return_type,
    )

    ray_method_type.type.names["remote"] = remote_node
    return ray_method_type


class RayActorPlugin(Plugin):
    def get_class_decorator_hook(self, fullname: str):
        """Add type information to Ray actor classes.

        This hook performs inplace transform by adding a new method to the user
        class with signature `.remote(...args_from_init...) -> ActorHandle[Cls]
        """
        if fullname == "ray.worker.remote":
            return add_remote_method_to_class

    def get_attribute_hook(self, fullname: str):
        """Dynamically generate .remote information given handle.method access
        """

        if fullname.startswith("ray.actor.ActorHandle"):
            return partial(generate_actor_method_type, plugin_obj=self)
