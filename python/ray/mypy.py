import pdb

from typing import List, Optional, Union, cast

from mypy.fixup import TypeFixer
from mypy.nodes import (ARG_POS, MDEF, SYMBOL_FUNCBASE_TYPES, Argument, Block,
                        CallExpr, ClassDef, Expression, FuncDef, JsonDict,
                        PassStmt, RefExpr, SymbolTableNode, Var)
from mypy.plugin import (AttributeContext, ClassDefContext, FunctionContext,
                         MethodContext, Plugin,
                         SemanticAnalyzerPluginInterface)
# from mypy.plugins.common import add_method_to_class
from mypy.semanal import set_callable_name
from mypy.semanal_shared import SemanticAnalyzerInterface
from mypy.typeops import \
    try_getting_str_literals  # noqa: F401  # Part of public API
from mypy.types import (CallableType, Instance, NoneType, Overloaded, Type,
                        TypeType, TypeVarDef, deserialize_type,
                        get_proper_type)
from mypy.typevars import fill_typevars
from mypy.util import get_unique_redefinition_name
from ray.actor import ActorClass


# Source: modified from mypy's plugin.common
def add_method_to_class(api: SemanticAnalyzerPluginInterface,
                        cls: ClassDef,
                        name: str,
                        args: List[Argument],
                        return_type: Type,
                        self_type: Optional[Type] = None,
                        tvar_def: Optional[TypeVarDef] = None,
                        is_static_method: bool = False) -> None:
    """Adds a new method to a class definition.
    """
    info = cls.info

    # First remove any previously generated methods with the same name
    # to avoid clashes and problems in the semantic analyzer.
    if name in info.names:
        sym = info.names[name]
        if sym.plugin_generated and isinstance(sym.node, FuncDef):
            cls.defs.body.remove(sym.node)

    self_type = self_type or fill_typevars(info)
    function_type = api.named_type('__builtins__.function')

    if not is_static_method:
        args = [Argument(Var('self'), self_type, None, ARG_POS)] + args
    arg_types, arg_names, arg_kinds = [], [], []
    for arg in args:
        assert arg.type_annotation, 'All arguments must be fully typed.'
        arg_types.append(arg.type_annotation)
        arg_names.append(arg.variable.name)
        arg_kinds.append(arg.kind)

    signature = CallableType(arg_types, arg_kinds, arg_names, return_type,
                             function_type)
    if tvar_def:
        signature.variables = [tvar_def]

    func = FuncDef(name, args, Block([PassStmt()]))
    func.info = info
    func.type = set_callable_name(signature, func)
    func._fullname = info.fullname + '.' + name
    func.line = info.line

    # NOTE: we would like the plugin generated node to dominate, but we still
    # need to keep any existing definitions so they get semantically analyzed.
    if name in info.names:
        # Get a nice unique name instead.
        r_name = get_unique_redefinition_name(name, info.names)
        info.names[r_name] = info.names[name]

    info.names[name] = SymbolTableNode(MDEF, func, plugin_generated=True)
    info.defn.defs.body.append(func)


def plugin(version: str):
    print("Ray plugin called! ", version)
    return RayActorPlugin


class RayActorPlugin(Plugin):
    def get_method_hook(self, fullname: str):
        if fullname.startswith("ray.actor.ActorMethod"):

            def callback(ctx: MethodContext):
                print("method_hook called with ", fullname, ctx)
                return ctx.default_return_type

            return callback

    def get_attribute_hook(self, fullname: str):

        if fullname.startswith("ray.actor.ActorHandle"):

            def callback(ctx: AttributeContext):
                print("attribute hook called with ", fullname, type(ctx.type))

                trying_to_get = ctx.context.name

                original_actor_cls = ctx.type.args[0].item.type
                print(original_actor_cls, type(original_actor_cls))
                actor_cls_names = original_actor_cls.names

                found: Optional[SymbolTableNode] = actor_cls_names.get(
                    trying_to_get)
                if found is None:
                    ctx.api.fail(f"Can't find actor method {trying_to_get}",
                                 ctx.context)

                method = self.lookup_fully_qualified("ray.actor.ActorMethod")
                method_type = Instance(method.node, args=[])

                found = found.copy()
                # TODO: ^ instead of using copy, just create a new def with FuncDef
                # overwrite the name with .remote()
                self_arg = found.node.arguments[0]
                assert self_arg.variable.name == "self"
                # self_type = method.node
                self_type = method_type
                found.node.arguments[0] = Argument(
                    Var("self"), self_type, None, ARG_POS)

                node_type = found.node.type
                print(node_type.ret_type)
                print(node_type.ret_type.type.name)
                new_ret_type = node_type.ret_type
                # Prevent double wrapping ObjectRef[ObjectRef[RetType]]
                # But this is very hacky
                if node_type.ret_type.type.name != "ObjectRef":
                    new_ret_type = Instance(
                        self.lookup_fully_qualified("ray._raylet.ObjectRef")
                        .node,
                        args=[node_type.ret_type])
                found.node.type = node_type.copy_modified(
                    arg_types=[method_type] + node_type.arg_types[1:],
                    ret_type=new_ret_type)

                # pdb.set_trace()

                method_type.type.names["remote"] = found

                # print(method_type, type(method_type))
                # print(method_type.type, type(method_type.type))
                # print(method_type.type.get_method("remote"))

                return method_type
                # modify constructure
                new_out = self.lookup_fully_qualified("b.A").node.get(
                    "call").type.copy_modified(
                        arg_types=[], arg_kinds=[], arg_names=[])
                print(new_out)
                return new_out

            return callback

    def get_class_decorator_hook(self, fullname: str):
        if fullname == "ray.worker.remote":

            def callback(ctx: ClassDefContext):
                print("cls dec called")

                import pdb
                # pdb.set_trace()
                # actor_class_name = ctx.cls.fullname
                # actor_def = ctx.api.lookup_fully_qualified(actor_class_name)
                actor_cls_def = ctx.api.lookup_fully_qualified_or_none(
                    "ray.actor.ActorHandle")
                if actor_cls_def is None:
                    ctx.api.defer()
                print(actor_cls_def)
                print(type(actor_cls_def.node))

                # remote_method = FuncDef("remote", [], body=Block([]))
                # ctx.cls.defs.body.clear()
                # ctx.cls.info.names.clear()
                # new_call_signature = (
                #     ctx.cls.info.get("call").type.copy_modified(
                #         arg_types=[], arg_kinds=[], arg_names=[]))

                info = ctx.cls.info
                method_names = [f.name for f in info.defn.defs.body]
                assert "__init__" not in method_names, NotImplementedError()

                print(method_names)
                add_method_to_class(
                    ctx.api,
                    ctx.cls,
                    "remote",
                    args=[],
                    return_type=Instance(
                        actor_cls_def.node,
                        args=[TypeType(Instance(ctx.cls.info, args=[]))]),
                    is_static_method=True)
                ctx.cls.info.get('remote').node.is_static = True

                # ctx.cls.info.get("call").node.arguments[1],
                # ctx.cls.info.get("call").node.arguments[1],
                # del info.names["call"]
                # info.defn.defs.body.pop(0)

                print(ctx.cls.info)
                print(ctx.cls)

            return callback
