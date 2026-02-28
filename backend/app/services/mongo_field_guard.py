from __future__ import annotations

from typing import Any, Dict, Iterable, Set


def _collect_allowed_paths(schema: Dict[str, Any]) -> Set[str]:
    """
    Accepts schema from mongo_schema.infer_schema().

    Supports both styles:
    - {"fields": [{"field": "region", ...}, ...]}
    - {"fields": [{"path": "items[].price", ...}, ...]}

    Returns a set of allowed field paths as they would appear in Mongo:
      - "region"
      - "order_date"
      - "items.price"   (arrays normalized to dot paths)
    """
    allowed: Set[str] = set()

    for f in schema.get("fields", []) or []:
        if not isinstance(f, dict):
            continue
        name = f.get("path") or f.get("field")
        if not name or not isinstance(name, str):
            continue

        # Normalize: "items[].price" -> "items.price"
        name = name.replace("[]", "")
        # Remove accidental leading dots
        name = name.lstrip(".")
        allowed.add(name)

    return allowed


def _is_field_ref(v: Any) -> bool:
    return isinstance(v, str) and v.startswith("$") and len(v) > 1


def _strip_field_ref(v: str) -> str:
    # "$region" -> "region"
    # "$$ROOT" or "$$NOW" should be treated separately (system vars)
    return v[1:]


def _is_system_var(field_ref: str) -> bool:
    # $$ROOT, $$NOW, etc.
    return field_ref.startswith("$$")


def _validate_field_path(field: str, allowed: Set[str]) -> None:
    """
    Validate that a field exists in allowed set.
    Accepts dotted paths like "a.b.c".
    For MVP: require exact match OR prefix match when schema captured deeper paths.
    """
    # exact match
    if field in allowed:
        return

    # If schema contains deeper paths, allow using prefix fields only if present.
    # Example: allowed has "customer.address.city" and pipeline uses "customer.address.city"
    # but not "customer" — should fail (strict) OR allow? We keep strict.
    raise ValueError(f"MongoFieldGuard: unknown field referenced: '{field}'")


def validate_fields_in_spec(spec: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """
    Walk the spec recursively and validate every "$field" reference
    against the inferred schema allow-list.

    - Validates:
      "$group": {"_id": "$region", ...}
      "$sum": "$total_amount"
      "$match": {"region": "West"}   (keys are fields too)
      "$project": {"region": 1, "x": "$total_amount"}

    - Skips:
      system vars like "$$ROOT"
      operators like "$gte", "$and" (keys that start with $)
    """
    allowed = _collect_allowed_paths(schema)

    if not allowed:
        # If schema inference returned no fields, do not crash.
        # You can choose to raise instead for strict mode.
        return

    qtype = (spec.get("query_type") or "").lower().strip()

    if qtype == "find":
        _walk_filter_or_expr(spec.get("filter") or {}, allowed)
        proj = spec.get("projection")
        if isinstance(proj, dict):
            _walk_project(proj, allowed)
        sort = spec.get("sort")
        if isinstance(sort, dict):
            for k in sort.keys():
                if isinstance(k, str) and not k.startswith("$"):
                    _validate_field_path(k.replace("[]", ""), allowed)

    elif qtype == "aggregate":
        pipeline = spec.get("pipeline") or []
        if not isinstance(pipeline, list):
            return
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                continue
            stage_name, stage_body = next(iter(stage.items()))
            if stage_name == "$match":
                if isinstance(stage_body, dict):
                    _walk_filter_or_expr(stage_body, allowed)
            elif stage_name == "$project":
                if isinstance(stage_body, dict):
                    _walk_project(stage_body, allowed)
            elif stage_name == "$group":
                if isinstance(stage_body, dict):
                    _walk_group(stage_body, allowed)
            elif stage_name == "$sort":
                if isinstance(stage_body, dict):
                    for k in stage_body.keys():
                        if isinstance(k, str) and not k.startswith("$"):
                            _validate_field_path(k.replace("[]", ""), allowed)
            elif stage_name == "$limit":
                pass
            else:
                # other stages validated elsewhere (stage allow-list)
                pass


def _walk_group(group_doc: Dict[str, Any], allowed: Set[str]) -> None:
    # _id can be "$field" or object
    _id = group_doc.get("_id")
    _walk_expr(_id, allowed)

    for k, v in group_doc.items():
        if k == "_id":
            continue
        _walk_expr(v, allowed)


def _walk_project(project_doc: Dict[str, Any], allowed: Set[str]) -> None:
    for out_field, expr in project_doc.items():
        # Out field name is not a source field; it’s output key.
        # But if they use dotted out keys, it’s still okay.
        _walk_expr(expr, allowed)


def _walk_filter_or_expr(obj: Any, allowed: Set[str]) -> None:
    """
    For $match / find filter:
    - keys that DO NOT start with '$' are field names -> validate
    - values may contain operators -> walk recursively
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and not k.startswith("$"):
                _validate_field_path(k.replace("[]", ""), allowed)
            _walk_filter_or_expr(v, allowed)
    elif isinstance(obj, list):
        for x in obj:
            _walk_filter_or_expr(x, allowed)
    else:
        # primitives ok
        return


def _walk_expr(obj: Any, allowed: Set[str]) -> None:
    """
    Walk general expressions ($sum, $avg, $cond, etc).
    Validate "$field" references.
    """
    if _is_field_ref(obj):
        if _is_system_var(obj):
            return
        field = _strip_field_ref(obj)
        field = field.replace("[]", "")
        _validate_field_path(field, allowed)
        return

    if isinstance(obj, dict):
        for k, v in obj.items():
            # operator keys start with $ — not a field name
            if isinstance(k, str) and not k.startswith("$"):
                # In some expressions, dict keys can be field names (rare), but safe to validate.
                _validate_field_path(k.replace("[]", ""), allowed)
            _walk_expr(v, allowed)

    elif isinstance(obj, list):
        for x in obj:
            _walk_expr(x, allowed)
