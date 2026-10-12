"""JSON Schema and README key table, generated from the config dataclasses.

`scripts/generate_config_schema.py` writes both; the tests fail while either is
stale. Descriptions are the field docstrings in `src.config`.
"""

import ast
from collections.abc import Iterator
import dataclasses
import inspect
from itertools import pairwise
import json
from pathlib import Path
import textwrap
import types
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    get_args,
    get_origin,
    get_type_hints,
)

from dature.validators.compare import ComparePredicate, LengthComparePredicate
from dature.validators.predicate import AndPredicate, Predicate

from src.config_validators import SECURE_URL_PATTERN, SecureUrl, Template, Timezone

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

SCHEMA_DIALECT = "http://json-schema.org/draft-07/schema#"
README_START = "<!-- config-table:start -->"
README_END = "<!-- config-table:end -->"
REGENERATE_COMMAND = "uv run python -m scripts.generate_config_schema"

# Python type -> (JSON Schema type, README wording)
_SCALARS: dict[object, tuple[str, str]] = {
    str: ("string", "String"),
    int: ("integer", "Integer"),
    float: ("number", "Number"),
    Path: ("string", "Path"),
}
_COMPARE_KEYWORDS = {
    "ge": "minimum",
    "gt": "exclusiveMinimum",
    "le": "maximum",
    "lt": "exclusiveMaximum",
}


@dataclasses.dataclass(frozen=True, slots=True)
class _Field:
    name: str
    base: Any  # the annotation without `Annotated` and `| None`
    optional: bool
    predicates: tuple[Predicate, ...]
    doc: str
    required: bool
    default: object  # `dataclasses.MISSING` when there is no literal default


def _field_docs(cls: "type[DataclassInstance]") -> dict[str, str]:
    """Attribute docstrings: the string literal right below each field."""
    (node,) = ast.parse(textwrap.dedent(inspect.getsource(cls))).body

    if not isinstance(node, ast.ClassDef):
        raise TypeError(f"the source of {cls.__qualname__} is not a class")

    docs: dict[str, str] = {}

    for statement, following in pairwise(node.body):
        if (
            isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and isinstance(following, ast.Expr)
            and isinstance(following.value, ast.Constant)
            and isinstance(following.value.value, str)
        ):
            docs[statement.target.id] = " ".join(following.value.value.split())

    return docs


def _flatten(predicate: Predicate) -> Iterator[Predicate]:
    if isinstance(predicate, AndPredicate):
        yield from _flatten(predicate.left)
        yield from _flatten(predicate.right)

    else:
        yield predicate


def _fields(cls: "type[DataclassInstance]") -> list[_Field]:
    hints = get_type_hints(cls, include_extras=True)
    docs = _field_docs(cls)
    result: list[_Field] = []

    for item in dataclasses.fields(cls):
        if item.name not in docs:
            raise ValueError(f"{cls.__qualname__}.{item.name} has no docstring")

        hint = hints[item.name]
        predicates: tuple[Predicate, ...] = ()

        if get_origin(hint) is Annotated:
            hint, *metadata = get_args(hint)
            predicates = tuple(
                flat
                for extra in metadata
                if isinstance(extra, Predicate)
                for flat in _flatten(extra)
            )

        args = get_args(hint)
        optional = get_origin(hint) is types.UnionType and type(None) in args

        if optional:
            (hint,) = (arg for arg in args if arg is not type(None))

            if dataclasses.is_dataclass(hint):
                # dature 1.6 reports any error inside an optional section as
                # "'NoneType' object has no attribute '__name__'", without a key.
                raise TypeError(
                    f"{cls.__qualname__}.{item.name}: give the section a default "
                    "instead of making it optional"
                )

        result.append(
            _Field(
                name=item.name,
                base=hint,
                optional=optional,
                predicates=predicates,
                doc=docs[item.name],
                required=item.default is dataclasses.MISSING
                and item.default_factory is dataclasses.MISSING,
                default=item.default,
            )
        )

    return result


def _description(item: _Field) -> str:
    for predicate in item.predicates:
        if isinstance(predicate, Template):
            names = ", ".join(f"`{{{name}}}`" for name in predicate.placeholders)

            return f"{item.doc} Placeholders: {names}."

    return item.doc


def _json_value(value: object) -> object:
    if isinstance(value, Path):
        return value.as_posix()  # the same on Windows and on the Linux CI

    return value


def _apply(predicate: Predicate, schema: dict[str, Any]) -> None:
    if isinstance(predicate, ComparePredicate):
        schema[_COMPARE_KEYWORDS[predicate.op]] = predicate.value

    elif isinstance(predicate, LengthComparePredicate) and predicate.op == "ge":
        keyword = "minItems" if schema["type"] == "array" else "minLength"
        schema[keyword] = predicate.value

    elif isinstance(predicate, SecureUrl):
        schema["pattern"] = SECURE_URL_PATTERN

    elif not isinstance(predicate, Template | Timezone):
        # Templates and timezones are checked at load only; the description
        # of a template field lists its placeholders.
        raise TypeError(f"no JSON Schema mapping for {predicate!r}")


def _type_schema(tp: Any) -> dict[str, Any]:  # noqa: ANN401
    if isinstance(tp, type) and dataclasses.is_dataclass(tp):
        return _object_schema(tp)

    if tp in _SCALARS:
        return {"type": _SCALARS[tp][0]}

    origin = get_origin(tp)

    if origin is Literal:
        return {"enum": list(get_args(tp))}

    if origin is tuple and get_args(tp)[1:] == (Ellipsis,):
        return {"type": "array", "items": _type_schema(get_args(tp)[0])}

    raise TypeError(f"no JSON Schema mapping for {tp!r}")


def _object_schema(cls: "type[DataclassInstance]") -> dict[str, Any]:
    properties: dict[str, Any] = {}
    required: list[str] = []

    for item in _fields(cls):
        schema = _type_schema(item.base)

        for predicate in item.predicates:
            _apply(predicate, schema)

        if item.optional:
            schema = {"anyOf": [schema, {"type": "null"}]}

        schema = {"description": _description(item), **schema}

        if item.default is not dataclasses.MISSING:
            schema["default"] = _json_value(item.default)

        if item.required:
            required.append(item.name)

        properties[item.name] = schema

    result: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }

    if required:
        result["required"] = required

    return result


def build_schema(cls: "type[DataclassInstance]") -> dict[str, Any]:
    return {
        "$schema": SCHEMA_DIALECT,
        "$comment": f"Generated from src/config.py by `{REGENERATE_COMMAND}`.",
        "title": cls.__name__,
        "description": inspect.getdoc(cls),
        **_object_schema(cls),
    }


def render_schema_json(cls: "type[DataclassInstance]") -> str:
    return json.dumps(build_schema(cls), indent=2, ensure_ascii=False) + "\n"


def _type_text(tp: Any) -> str:  # noqa: ANN401
    if tp in _SCALARS:
        return _SCALARS[tp][1]

    origin = get_origin(tp)

    if origin is Literal:
        return "One of " + ", ".join(f"`{value}`" for value in get_args(tp))

    if origin is tuple:
        return f"List of {_type_text(get_args(tp)[0]).lower()}s"

    raise TypeError(f"no README wording for {tp!r}")


def _rows(cls: "type[DataclassInstance]", prefix: str) -> Iterator[tuple[str, ...]]:
    for item in _fields(cls):
        key = f"{prefix}{item.name}"

        if isinstance(item.base, type) and dataclasses.is_dataclass(item.base):
            yield from _rows(item.base, f"{key}.")

            continue

        type_text = _type_text(item.base) + (" or `null`" if item.optional else "")
        default = (
            "required"
            if item.required
            else f"`{json.dumps(_json_value(item.default), ensure_ascii=False)}`"
        )

        yield f"`{key}`", type_text, default, _description(item)


def render_readme_table(cls: "type[DataclassInstance]") -> str:
    lines = ["| Key | Type | Default | Description |", "| --- | --- | --- | --- |"]
    lines += [
        "| " + " | ".join(cell.replace("|", "\\|") for cell in row) + " |"
        for row in _rows(cls, "")
    ]

    return "\n".join(lines) + "\n"


def splice_readme(readme: str, table: str) -> str:
    """Replace what sits between the README markers with `table`."""
    start = readme.index(README_START) + len(README_START)
    end = readme.index(README_END)

    return f"{readme[:start]}\n{table}{readme[end:]}"
