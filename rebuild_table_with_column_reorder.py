import pyodbc
from typing import List, Dict, Literal

PositionType = Literal["before", "after"]

def get_connection(
    server: str,
    database: str,
    driver: str = "{ODBC Driver 17 for SQL Server}",
    trusted_connection: bool = True,
    username: str = None,
    password: str = None,
):
    if trusted_connection:
        conn_str = (
            f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};"
        )
    return pyodbc.connect(conn_str)


def fetch_ordered_metadata(cursor, schema: str, table: str) -> List[dict]:
    """
    Return a list of existing columns with full metadata, ordered by column_id.
    """
    sql = """
    SELECT
        c.column_id,
        c.name AS column_name,
        t.name AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable,
        c.is_identity,
        dc.definition AS default_definition
    FROM sys.columns AS c
    INNER JOIN sys.tables AS tb ON c.object_id = tb.object_id
    INNER JOIN sys.types AS t ON c.user_type_id = t.user_type_id
    LEFT JOIN sys.default_constraints AS dc
        ON c.default_object_id = dc.object_id
    WHERE SCHEMA_NAME(tb.schema_id) = ?
      AND tb.name = ?
    ORDER BY c.column_id;
    """
    cols = []
    for row in cursor.execute(sql, schema, table):
        data_type = row.data_type.upper()
        # Build full type string
        if data_type in ("CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "BINARY", "VARBINARY"):
            if row.max_length == -1:
                type_str = f"{data_type}(MAX)"
            else:
                length = row.max_length
                if data_type in ("NCHAR", "NVARCHAR"):
                    length = int(length / 2)
                type_str = f"{data_type}({length})"
        elif data_type in ("DECIMAL", "NUMERIC"):
            type_str = f"{data_type}({row.precision},{row.scale})"
        elif data_type in ("DATETIME2", "TIME", "DATETIMEOFFSET"):
            type_str = f"{data_type}({row.scale})"
        else:
            type_str = data_type

        cols.append(
            {
                "name": row.column_name,
                "type": type_str,
                "is_nullable": bool(row.is_nullable),
                "is_identity": bool(row.is_identity),
                "default": row.default_definition,  # already T-SQL expression or None
            }
        )
    return cols


def build_col_def(col: dict) -> str:
    """
    Build a column definition string from metadata dict with keys:
    name, type, is_nullable, is_identity, default.
    """
    nullable = "NULL" if col["is_nullable"] else "NOT NULL"
    identity = " IDENTITY(1,1)" if col["is_identity"] else ""
    default = f" DEFAULT {col['default']}" if col.get("default") else ""
    return f"[{col['name']}] {col['type']}{identity}{default} {nullable}"


def insert_new_columns(
    existing_cols: List[dict],
    new_columns: List[dict],
) -> List[dict]:
    """
    existing_cols: list of dicts from fetch_ordered_metadata (current order preserved)
    new_columns: list of dicts like:
        {
            "name": "NewCol",
            "type": "INT",                 # full T-SQL type for the new column
            "nullable": True,
            "default": "0",                # T-SQL expression or None
            "position": "after",           # "before" or "after"
            "anchor": "ExistingColName"    # required: where to insert
        }

    Returns a new ordered list of column dicts, including new ones.
    """
    # Create a working list of just names for position logic
    ordered = existing_cols.copy()

    for new_col in new_columns:
        anchor = new_col["anchor"]
        pos = new_col["position"]
        if pos not in ("before", "after"):
            raise ValueError("position must be 'before' or 'after'")

        # Build metadata dict in same shape as existing
        new_meta = {
            "name": new_col["name"],
            "type": new_col["type"],
            "is_nullable": new_col.get("nullable", True),
            "is_identity": new_col.get("identity", False),
            "default": new_col.get("default"),
        }

        # Find anchor index
        idx = next(
            (i for i, c in enumerate(ordered) if c["name"].lower() == anchor.lower()),
            None,
        )
        if idx is None:
            raise ValueError(f"Anchor column '{anchor}' not found")

        insert_idx = idx + 1 if pos == "after" else idx
        # Insert new column metadata at the calculated position
        ordered.insert(insert_idx, new_meta)

    return ordered


def rebuild_with_inferred_and_inserted_columns(
    server: str,
    database: str,
    schema: str,
    table: str,
    new_columns: List[dict],
    tmp_suffix: str = "_TmpReorder",
    driver: str = "{ODBC Driver 17 for SQL Server}",
    trusted_connection: bool = True,
    username: str = None,
    password: str = None,
):
    """
    new_columns: list of new column specs (see insert_new_columns docstring).

    The script:
    - Reads current table definition
    - Inserts new columns at requested positions (relative to current columns)
    - Creates a temp table with that physical order
    - Copies data, populating new columns from default or NULL
    - Swaps the temp table in
    """
    full_table = f"[{schema}].[{table}]"
    tmp_table_name = f"{table}{tmp_suffix}"
    full_tmp_table = f"[{schema}].[{tmp_table_name}]"

    with get_connection(server, database, driver, trusted_connection, username, password) as conn:
        cur = conn.cursor()

        # 1. Read existing metadata in current order
        existing_cols = fetch_ordered_metadata(cur, schema, table)

        # 2. Build new ordered column list with inserted new columns
        new_ordered_cols = insert_new_columns(existing_cols, new_columns)

        # 3. CREATE TABLE for tmp with same attributes for old cols + definitions for new
        col_defs = [build_col_def(c) for c in new_ordered_cols]
        create_sql = (
            f"CREATE TABLE {full_tmp_table} (\n    "
            + ",\n    ".join(col_defs)
            + "\n);"
        )

        # 4. Build INSERT ... SELECT mapping
        insert_cols = [f"[{c['name']}]" for c in new_ordered_cols]
        select_exprs = []
        existing_names = {c["name"].lower() for c in existing_cols}

        for c in new_ordered_cols:
            col_name = c["name"]
            if col_name.lower() in existing_names:
                select_exprs.append(f"[{col_name}]")
            else:
                # it's one of the new columns – find its spec
                spec = next(n for n in new_columns if n["name"].lower() == col_name.lower())
                default = spec.get("default")
                if default is not None:
                    select_exprs.append(f"{default} AS [{col_name}]")
                else:
                    select_exprs.append(f"NULL AS [{col_name}]")

        # If identities exist, we need to handle IDENTITY_INSERT; simplest:
        has_identity = any(c["is_identity"] for c in new_ordered_cols)
        insert_batch_parts = []
        if has_identity:
            insert_batch_parts.append(f"SET IDENTITY_INSERT {full_tmp_table} ON;")

        insert_batch_parts.append(
            f"INSERT INTO {full_tmp_table} ({', '.join(insert_cols)})\n"
            f"SELECT {', '.join(select_exprs)}\n"
            f"FROM {full_table} WITH (HOLDLOCK TABLOCKX);"
        )

        if has_identity:
            insert_batch_parts.append(f"SET IDENTITY_INSERT {full_tmp_table} OFF;")

        insert_sql = "\n".join(insert_batch_parts)

        # 5. Execute migration (DDL only; indexes/FKs not handled here)
        cur.execute(create_sql)
        cur.execute(insert_sql)
        cur.execute(f"DROP TABLE {full_table};")
        cur.execute(f"EXEC sp_rename N'{full_tmp_table}', N'{table}', 'OBJECT';")
        conn.commit()
