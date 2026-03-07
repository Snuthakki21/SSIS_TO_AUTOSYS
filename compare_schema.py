#!/usr/bin/env python3
"""
SQL Schema Comparison Tool

Compares two SQL schema files (PROD and DEV) and generates SQL to update DEV to match PROD.
Produces differences.sql, JSON report, and Markdown report.

Usage:
    python compare_schema.py --prod prod.sql --dev dev.sql --out differences.sql

Exit Codes:
    0 - No differences found
    1 - Differences found and files generated
    2 - Parse error or fatal error
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any


# =============================================================================
# CONSTANTS
# =============================================================================

VERSION = "1.0.0"
DEFAULT_ENCODING = "utf-8"

# SQL Server reserved keywords that might be used as identifiers
RESERVED_KEYWORDS = {
    'add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'authorization', 'backup',
    'begin', 'between', 'break', 'browse', 'bulk', 'by', 'cascade', 'case',
    'check', 'checkpoint', 'close', 'clustered', 'coalesce', 'collate', 'column',
    'commit', 'compute', 'constraint', 'contains', 'containstable', 'continue',
    'convert', 'create', 'cross', 'current', 'current_date', 'current_time',
    'current_timestamp', 'current_user', 'cursor', 'database', 'dbcc', 'deallocate',
    'declare', 'default', 'delete', 'deny', 'desc', 'disk', 'distinct', 'distributed',
    'double', 'drop', 'dump', 'else', 'end', 'errlvl', 'escape', 'except', 'exec',
    'execute', 'exists', 'exit', 'external', 'fetch', 'file', 'fillfactor', 'for',
    'foreign', 'freetext', 'freetexttable', 'from', 'full', 'function', 'goto',
    'grant', 'group', 'having', 'holdlock', 'identity', 'identity_insert',
    'identitycol', 'if', 'in', 'index', 'inner', 'insert', 'intersect', 'into',
    'is', 'join', 'key', 'kill', 'left', 'like', 'lineno', 'load', 'merge',
    'national', 'nocheck', 'nonclustered', 'not', 'null', 'nullif', 'of', 'off',
    'offsets', 'on', 'open', 'opendatasource', 'openquery', 'openrowset', 'openxml',
    'option', 'or', 'order', 'outer', 'over', 'percent', 'pivot', 'plan', 'precision',
    'primary', 'print', 'proc', 'procedure', 'public', 'raiserror', 'read', 'readtext',
    'reconfigure', 'references', 'replication', 'restore', 'restrict', 'return',
    'revert', 'revoke', 'right', 'rollback', 'rowcount', 'rowguidcol', 'rule',
    'save', 'schema', 'securityaudit', 'select', 'semantickeyphrasetable',
    'semanticsimilaritydetailstable', 'semanticsimilaritytable', 'session_user',
    'set', 'setuser', 'shutdown', 'some', 'statistics', 'system_user', 'table',
    'tablesample', 'textsize', 'then', 'to', 'top', 'tran', 'transaction', 'trigger',
    'truncate', 'try_convert', 'tsequal', 'union', 'unique', 'unpivot', 'update',
    'updatetext', 'use', 'user', 'values', 'varying', 'view', 'waitfor', 'when',
    'where', 'while', 'with', 'within', 'writetext'
}


# =============================================================================
# ENUMS
# =============================================================================

class ObjectType(Enum):
    """Types of SQL objects that can be parsed and compared."""
    TABLE = "TABLE"
    VIEW = "VIEW"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    TRIGGER = "TRIGGER"
    INDEX = "INDEX"
    CONSTRAINT = "CONSTRAINT"
    UNKNOWN = "UNKNOWN"


class DifferenceType(Enum):
    """Types of differences that can be detected."""
    ONLY_IN_PROD = "only_in_prod"
    ONLY_IN_DEV = "only_in_dev"
    MODIFIED = "modified"
    MANUAL_REVIEW = "manual_review"


class ChangeRisk(Enum):
    """Risk level for generated changes."""
    SAFE = "safe"
    CAUTION = "caution"
    MANUAL_REVIEW = "manual_review"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ColumnDefinition:
    """Represents a column definition in a table."""
    name: str
    data_type: str
    nullable: bool = True
    default_value: Optional[str] = None
    is_identity: bool = False
    identity_seed: Optional[int] = None
    identity_increment: Optional[int] = None
    is_computed: bool = False
    computed_expression: Optional[str] = None
    length: Optional[str] = None  # For varchar/nvarchar/decimal precision
    
    def normalize(self) -> str:
        """Generate normalized representation for comparison."""
        parts = [self.name.lower(), self.data_type.lower()]
        if self.length:
            parts.append(f"({self.lower()})")
        if self.is_identity:
            parts.append("identity")
        if self.is_computed:
            parts.append(f"computed:{self.computed_expression.lower() if self.computed_expression else ''}")
        if self.default_value:
            parts.append(f"default:{self.default_value.lower()}")
        parts.append("null" if self.nullable else "notnull")
        return "|".join(parts)


@dataclass
class ConstraintDefinition:
    """Represents a table constraint."""
    name: Optional[str]
    constraint_type: str  # PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, DEFAULT
    columns: List[str] = field(default_factory=list)
    reference_table: Optional[str] = None
    reference_columns: List[str] = field(default_factory=list)
    check_expression: Optional[str] = None
    is_clustered: bool = False
    
    def normalize(self) -> str:
        """Generate normalized representation for comparison."""
        parts = [self.constraint_type.lower()]
        if self.name:
            parts.append(self.name.lower())
        parts.extend(c.lower() for c in sorted(self.columns))
        if self.reference_table:
            parts.append(f"ref:{self.reference_table.lower()}")
            parts.extend(c.lower() for c in self.reference_columns)
        if self.check_expression:
            parts.append(f"check:{self.check_expression.lower()}")
        if self.is_clustered:
            parts.append("clustered")
        return "|".join(parts)


@dataclass
class IndexDefinition:
    """Represents a table index."""
    name: str
    columns: List[str]
    is_unique: bool = False
    is_clustered: bool = False
    is_primary_key: bool = False
    included_columns: List[str] = field(default_factory=list)
    
    def normalize(self) -> str:
        """Generate normalized representation for comparison."""
        parts = [self.name.lower()]
        if self.is_unique:
            parts.append("unique")
        if self.is_clustered:
            parts.append("clustered")
        if self.is_primary_key:
            parts.append("pk")
        parts.extend(c.lower() for c in self.columns)
        if self.included_columns:
            parts.append("include")
            parts.extend(c.lower() for c in self.included_columns)
        return "|".join(parts)


@dataclass
class SqlObject:
    """Represents a parsed SQL object (table, view, procedure, function)."""
    object_type: ObjectType
    schema: str
    name: str
    raw_sql: str
    normalized_sql: str
    
    # For tables
    columns: Dict[str, ColumnDefinition] = field(default_factory=dict)
    constraints: List[ConstraintDefinition] = field(default_factory=list)
    indexes: List[IndexDefinition] = field(default_factory=list)
    
    # For views/procedures/functions
    body: Optional[str] = None
    
    @property
    def full_name(self) -> str:
        """Returns schema-qualified name."""
        return f"{self.schema}.{self.name}"
    
    @property
    def identity(self) -> str:
        """Returns unique identity for matching (type.schema.name)."""
        return f"{self.object_type.value}.{self.schema}.{self.name}"
    
    def normalize(self) -> str:
        """Generate normalized representation for comparison."""
        if self.object_type == ObjectType.TABLE:
            parts = ["table", self.schema.lower(), self.name.lower()]
            for col_name in sorted(self.columns.keys()):
                parts.append(self.columns[col_name].normalize())
            for constraint in sorted(self.constraints, key=lambda c: c.name or ''):
                parts.append(constraint.normalize())
            for idx in sorted(self.indexes, key=lambda i: i.name):
                parts.append(idx.normalize())
            return "|".join(parts)
        else:
            return self.normalized_sql


@dataclass
class ObjectDifference:
    """Represents a difference between PROD and DEV objects."""
    difference_type: DifferenceType
    object_type: ObjectType
    schema: str
    name: str
    prod_object: Optional[SqlObject] = None
    dev_object: Optional[SqlObject] = None
    details: Dict[str, Any] = field(default_factory=dict)
    risk_level: ChangeRisk = ChangeRisk.SAFE
    manual_review_reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "difference_type": self.difference_type.value,
            "object_type": self.object_type.value,
            "schema": self.schema,
            "name": self.name,
            "full_name": f"{self.schema}.{self.name}",
            "risk_level": self.risk_level.value,
            "manual_review_reason": self.manual_review_reason,
            "details": self.details
        }


@dataclass
class ComparisonResult:
    """Complete result of a schema comparison."""
    prod_objects: Dict[str, SqlObject] = field(default_factory=dict)
    dev_objects: Dict[str, SqlObject] = field(default_factory=dict)
    differences: List[ObjectDifference] = field(default_factory=list)
    parse_errors: List[str] = field(default_factory=list)
    
    def get_by_type(self, diff_type: DifferenceType) -> List[ObjectDifference]:
        """Get differences filtered by type."""
        return [d for d in self.differences if d.difference_type == diff_type]
    
    def get_by_object_type(self, obj_type: ObjectType) -> List[ObjectDifference]:
        """Get differences filtered by object type."""
        return [d for d in self.differences if d.object_type == obj_type]
    
    def get_manual_review_items(self) -> List[ObjectDifference]:
        """Get items requiring manual review."""
        return [d for d in self.differences if d.risk_level == ChangeRisk.MANUAL_REVIEW]
    
    @property
    def has_differences(self) -> bool:
        """Check if any differences were found."""
        return len(self.differences) > 0
    
    def summary_counts(self) -> Dict[str, int]:
        """Get summary counts of differences."""
        return {
            "objects_only_in_prod": len(self.get_by_type(DifferenceType.ONLY_IN_PROD)),
            "objects_only_in_dev": len(self.get_by_type(DifferenceType.ONLY_IN_DEV)),
            "modified_objects": len(self.get_by_type(DifferenceType.MODIFIED)),
            "manual_review_items": len(self.get_manual_review_items()),
            "modified_tables": len([d for d in self.differences 
                                   if d.difference_type == DifferenceType.MODIFIED 
                                   and d.object_type == ObjectType.TABLE]),
            "modified_views": len([d for d in self.differences 
                                  if d.difference_type == DifferenceType.MODIFIED 
                                  and d.object_type == ObjectType.VIEW]),
            "modified_procedures": len([d for d in self.differences 
                                       if d.difference_type == DifferenceType.MODIFIED 
                                       and d.object_type == ObjectType.PROCEDURE]),
            "modified_functions": len([d for d in self.differences 
                                      if d.difference_type == DifferenceType.MODIFIED 
                                      and d.object_type == ObjectType.FUNCTION]),
        }


# =============================================================================
# SQL NORMALIZATION
# =============================================================================

class SqlNormalizer:
    """Normalizes SQL for comparison purposes."""
    
    def __init__(self, ignore_comments: bool = True, ignore_case: bool = True):
        self.ignore_comments = ignore_comments
        self.ignore_case = ignore_case
    
    def normalize(self, sql: str) -> str:
        """Normalize SQL string for comparison."""
        result = sql
        
        # Remove comments first
        if self.ignore_comments:
            result = self._remove_comments(result)
        
        # Normalize line endings
        result = result.replace('\r\n', '\n').replace('\r', '\n')
        
        # Normalize bracketed identifiers to a consistent format
        result = self._normalize_identifiers(result)
        
        # Normalize whitespace
        result = self._normalize_whitespace(result)
        
        # Normalize GO separators
        result = self._normalize_go(result)
        
        # Normalize trailing semicolons
        result = self._normalize_semicolons(result)
        
        # Normalize CREATE OR ALTER to CREATE
        result = self._normalize_create_or_alter(result)
        
        # Convert to lowercase if ignoring case
        if self.ignore_case:
            result = result.lower()
        
        return result.strip()
    
    def _remove_comments(self, sql: str) -> str:
        """Remove both single-line and multi-line comments."""
        # Remove multi-line comments /* ... */
        result = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
        # Remove single-line comments -- ...
        result = re.sub(r'--[^\n]*', ' ', result)
        return result
    
    def _normalize_identifiers(self, sql: str) -> str:
        """Normalize bracketed [name] and quoted "name" identifiers."""
        # Remove brackets from identifiers [name] -> name
        result = re.sub(r'\[([^\]]+)\]', r'\1', sql)
        # Normalize double-quoted identifiers "name" -> name
        result = re.sub(r'"([^"]+)"', r'\1', result)
        return result
    
    def _normalize_whitespace(self, sql: str) -> str:
        """Collapse multiple whitespace to single space."""
        # Replace all whitespace sequences with single space
        result = re.sub(r'\s+', ' ', sql)
        return result
    
    def _normalize_go(self, sql: str) -> str:
        """Normalize GO batch separators."""
        # Normalize various GO formats
        result = re.sub(r'\bGO\b', 'GO', sql, flags=re.IGNORECASE)
        return result
    
    def _normalize_semicolons(self, sql: str) -> str:
        """Normalize trailing semicolons."""
        # Remove trailing semicolons (SQL Server doesn't require them)
        result = re.sub(r';\s*$', '', sql)
        return result
    
    def _normalize_create_or_alter(self, sql: str) -> str:
        """Normalize CREATE OR ALTER to CREATE for comparison."""
        # CREATE OR ALTER -> CREATE for comparison purposes
        result = re.sub(r'\bCREATE\s+OR\s+ALTER\b', 'CREATE', sql, flags=re.IGNORECASE)
        return result


# =============================================================================
# SQL PARSER
# =============================================================================

class SqlParser:
    """Parses SQL files into structured objects."""
    
    def __init__(self, normalizer: SqlNormalizer, debug: bool = False):
        self.normalizer = normalizer
        self.debug = debug
        self.errors: List[str] = []
    
    def parse_file(self, file_path: Path) -> Dict[str, SqlObject]:
        """Parse a SQL file and return dictionary of objects by identity."""
        self.errors = []
        
        try:
            content = file_path.read_text(encoding=DEFAULT_ENCODING)
        except Exception as e:
            self.errors.append(f"Failed to read {file_path}: {e}")
            return {}
        
        return self.parse_content(content)
    
    def parse_content(self, content: str) -> Dict[str, SqlObject]:
        """Parse SQL content and return dictionary of objects."""
        objects: Dict[str, SqlObject] = {}
        
        # Split into batches by GO
        batches = self._split_batches(content)
        
        for batch in batches:
            batch = batch.strip()
            if not batch:
                continue
            
            obj = self._parse_object(batch)
            if obj:
                if obj.identity in objects:
                    self.errors.append(f"Duplicate object found: {obj.identity}")
                else:
                    objects[obj.identity] = obj
        
        return objects
    
    def _split_batches(self, content: str) -> List[str]:
        """Split SQL content into batches separated by GO."""
        # Split on GO at line boundaries (case insensitive)
        pattern = r'(?:^|\n)\s*GO\s*(?:\n|$)'
        batches = re.split(pattern, content, flags=re.IGNORECASE)
        return [b.strip() for b in batches if b.strip()]
    
    def _parse_object(self, sql: str) -> Optional[SqlObject]:
        """Parse a single SQL statement into a SqlObject."""
        stripped = sql.strip()
        
        # Skip empty batches
        if not stripped or stripped.upper() == 'GO':
            return None
        
        # Skip if the batch is only comments (check by removing comments and seeing if anything remains)
        # Simple check: if it starts with comment and has no CREATE/ALTER keywords
        upper_stripped = stripped.upper()
        has_create = 'CREATE' in upper_stripped
        has_alter = 'ALTER' in upper_stripped
        
        if not has_create and not has_alter:
            # Likely just comments, skip
            return None
        
        normalized = self.normalizer.normalize(sql)
        
        # Try to identify object type
        obj_type = self._identify_object_type(sql)
        
        if obj_type == ObjectType.TABLE:
            return self._parse_table(sql, normalized)
        elif obj_type == ObjectType.VIEW:
            return self._parse_view(sql, normalized)
        elif obj_type == ObjectType.PROCEDURE:
            return self._parse_procedure(sql, normalized)
        elif obj_type == ObjectType.FUNCTION:
            return self._parse_function(sql, normalized)
        elif obj_type == ObjectType.TRIGGER:
            return self._parse_trigger(sql, normalized)
        elif obj_type == ObjectType.INDEX:
            return self._parse_index(sql, normalized)
        
        # Unknown object type - skip but log if debug
        if self.debug:
            preview = sql[:100].replace('\n', ' ')
            self.errors.append(f"Unrecognized object type: {preview}...")
        
        return None
    
    def _identify_object_type(self, sql: str) -> ObjectType:
        """Identify the type of SQL object from the statement."""
        upper_sql = sql.upper()
        
        # Check for CREATE TABLE
        if re.search(r'\bCREATE\s+TABLE\b', upper_sql):
            return ObjectType.TABLE
        
        # Check for CREATE VIEW
        if re.search(r'\bCREATE\s+(?:OR\s+ALTER\s+)?VIEW\b', upper_sql):
            return ObjectType.VIEW
        
        # Check for CREATE PROCEDURE/PROC
        if re.search(r'\bCREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\b', upper_sql):
            return ObjectType.PROCEDURE
        
        # Check for CREATE FUNCTION
        if re.search(r'\bCREATE\s+(?:OR\s+ALTER\s+)?FUNCTION\b', upper_sql):
            return ObjectType.FUNCTION
        
        # Check for CREATE TRIGGER
        if re.search(r'\bCREATE\s+(?:OR\s+ALTER\s+)?TRIGGER\b', upper_sql):
            return ObjectType.TRIGGER
        
        # Check for CREATE INDEX
        if re.search(r'\bCREATE\s+(?:UNIQUE\s+)?(?:CLUSTERED\s+|NONCLUSTERED\s+)?INDEX\b', upper_sql):
            return ObjectType.INDEX
        
        # Check for ALTER TABLE ADD CONSTRAINT
        if re.search(r'\bALTER\s+TABLE\b', upper_sql):
            # Could be constraint - treat as unknown for now
            pass
        
        return ObjectType.UNKNOWN
    
    def _extract_schema_name(self, sql: str) -> Tuple[str, str]:
        """Extract schema and object name from CREATE statement."""
        # Match patterns like: CREATE TABLE [dbo].[TableName] or CREATE TABLE dbo.TableName
        patterns = [
            # CREATE ... [schema].[name] or schema.name
            r'CREATE\s+(?:OR\s+ALTER\s+)?(?:TABLE|VIEW|PROCEDURE|PROC|FUNCTION|TRIGGER|INDEX)\s+'
            r'(?:\[?([^\].\s]+)\]?\.)?\[?([^\]\s(]+)\]?',
            # ALTER TABLE [schema].[name]
            r'ALTER\s+TABLE\s+(?:\[?([^\].\s]+)\]?\.)?\[?([^\]\s(]+)\]?',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                schema = match.group(1) or 'dbo'
                name = match.group(2)
                return schema, name
        
        return 'dbo', 'unknown'
    
    def _parse_table(self, sql: str, normalized: str) -> SqlObject:
        """Parse a CREATE TABLE statement."""
        schema, name = self._extract_schema_name(sql)
        
        obj = SqlObject(
            object_type=ObjectType.TABLE,
            schema=schema,
            name=name,
            raw_sql=sql,
            normalized_sql=normalized
        )
        
        # Extract column definitions
        obj.columns = self._parse_columns(sql)
        
        # Extract inline constraints
        obj.constraints = self._parse_inline_constraints(sql)
        
        # Extract indexes (may be separate statements, but try inline)
        obj.indexes = self._parse_inline_indexes(sql)
        
        return obj
    
    def _parse_columns(self, sql: str) -> Dict[str, ColumnDefinition]:
        """Parse column definitions from CREATE TABLE."""
        columns: Dict[str, ColumnDefinition] = {}
        
        # Extract content between parentheses
        match = re.search(r'CREATE\s+TABLE\s+[^(]+\((.*)\)', sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return columns
        
        content = match.group(1)
        
        # Split by commas, but be careful with nested parentheses
        column_defs = self._split_columns(content)
        
        for col_def in column_defs:
            col_def = col_def.strip()
            if not col_def:
                continue
            
            # Skip comment lines
            if col_def.startswith('--') or col_def.startswith('/*'):
                continue
            
            # Skip constraint definitions (they start with CONSTRAINT, PRIMARY, UNIQUE, etc.)
            if re.match(r'^(CONSTRAINT|PRIMARY|UNIQUE|FOREIGN|CHECK|INDEX)', col_def, re.IGNORECASE):
                continue
            
            column = self._parse_single_column(col_def)
            if column:
                columns[column.name] = column
        
        return columns
    
    def _split_columns(self, content: str) -> List[str]:
        """Split column definitions handling nested parentheses."""
        result = []
        current = []
        depth = 0
        
        for char in content:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                result.append(''.join(current))
                current = []
            else:
                current.append(char)
        
        if current:
            result.append(''.join(current))
        
        return result
    
    def _parse_single_column(self, col_def: str) -> Optional[ColumnDefinition]:
        """Parse a single column definition."""
        col_def = col_def.strip()
        
        # Skip if it looks like a comment
        if col_def.startswith('--') or col_def.startswith('/*'):
            return None
        
        # Pattern: [name] type [(length)] [NULL|NOT NULL] [IDENTITY...] [DEFAULT...]
        pattern = r'^(?:\[?([^\]\s]+)\]?)\s+'  # column name
        pattern += r'(\w+)'  # data type
        pattern += r'(?:\s*\(([^)]+)\))?'  # optional length/precision
        pattern += r'(?:\s+(IDENTITY)(?:\s*\(\s*(\d+)\s*,\s*(\d+)\s*\))?)?'  # identity
        pattern += r'(?:\s+(AS)\s+(.+))?'  # computed column
        pattern += r'(?:\s+(NULL|NOT\s+NULL))?'  # nullability
        pattern += r'(?:\s+DEFAULT\s+([^\s,]+))?'  # default value
        
        match = re.match(pattern, col_def, re.IGNORECASE)
        if not match:
            return None
        
        name = match.group(1)
        data_type = match.group(2)
        length = match.group(3)
        is_identity = match.group(4) is not None
        identity_seed = int(match.group(5)) if match.group(5) else None
        identity_increment = int(match.group(6)) if match.group(6) else None
        is_computed = match.group(7) is not None
        computed_expr = match.group(8)
        nullability = match.group(9)
        default_value = match.group(10)
        
        # Determine nullability
        nullable = True
        if nullability:
            nullable = nullability.upper() == 'NULL'
        
        return ColumnDefinition(
            name=name,
            data_type=data_type,
            nullable=nullable,
            default_value=default_value,
            is_identity=is_identity,
            identity_seed=identity_seed,
            identity_increment=identity_increment,
            is_computed=is_computed,
            computed_expression=computed_expr,
            length=length
        )
    
    def _parse_inline_constraints(self, sql: str) -> List[ConstraintDefinition]:
        """Parse inline constraints from CREATE TABLE."""
        constraints: List[ConstraintDefinition] = []
        
        # Extract content between parentheses
        match = re.search(r'CREATE\s+TABLE\s+[^(]+\((.*)\)', sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return constraints
        
        content = match.group(1)
        column_defs = self._split_columns(content)
        
        for col_def in column_defs:
            col_def = col_def.strip()
            
            # PRIMARY KEY constraint
            pk_match = re.search(r'(?:CONSTRAINT\s+(\[?[^\]]+\]?)\s+)?PRIMARY\s+KEY\s+(?:CLUSTERED\s+)?\(([^)]+)\)', 
                                col_def, re.IGNORECASE)
            if pk_match:
                constraint_name = pk_match.group(1)
                columns = [c.strip() for c in pk_match.group(2).split(',')]
                constraints.append(ConstraintDefinition(
                    name=constraint_name,
                    constraint_type='PRIMARY KEY',
                    columns=columns,
                    is_clustered=True
                ))
                continue
            
            # UNIQUE constraint
            unique_match = re.search(r'(?:CONSTRAINT\s+(\[?[^\]]+\]?)\s+)?UNIQUE\s+\(([^)]+)\)', 
                                    col_def, re.IGNORECASE)
            if unique_match:
                constraint_name = unique_match.group(1)
                columns = [c.strip() for c in unique_match.group(2).split(',')]
                constraints.append(ConstraintDefinition(
                    name=constraint_name,
                    constraint_type='UNIQUE',
                    columns=columns
                ))
                continue
            
            # FOREIGN KEY constraint
            fk_match = re.search(r'(?:CONSTRAINT\s+(\[?[^\]]+\]?)\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+(\S+)\s*\(([^)]+)\)', 
                                col_def, re.IGNORECASE)
            if fk_match:
                constraint_name = fk_match.group(1)
                columns = [c.strip() for c in fk_match.group(2).split(',')]
                ref_table = fk_match.group(3)
                ref_columns = [c.strip() for c in fk_match.group(4).split(',')]
                constraints.append(ConstraintDefinition(
                    name=constraint_name,
                    constraint_type='FOREIGN KEY',
                    columns=columns,
                    reference_table=ref_table,
                    reference_columns=ref_columns
                ))
                continue
            
            # CHECK constraint
            check_match = re.search(r'(?:CONSTRAINT\s+(\[?[^\]]+\]?)\s+)?CHECK\s*\(([^)]+)\)', 
                                   col_def, re.IGNORECASE)
            if check_match:
                constraint_name = check_match.group(1)
                expression = check_match.group(2)
                constraints.append(ConstraintDefinition(
                    name=constraint_name,
                    constraint_type='CHECK',
                    check_expression=expression
                ))
        
        return constraints
    
    def _parse_inline_indexes(self, sql: str) -> List[IndexDefinition]:
        """Parse inline indexes from CREATE TABLE."""
        indexes: List[IndexDefinition] = []
        # Most indexes are separate CREATE INDEX statements
        # This handles inline index definitions if present
        return indexes
    
    def _parse_view(self, sql: str, normalized: str) -> SqlObject:
        """Parse a CREATE VIEW statement."""
        schema, name = self._extract_schema_name(sql)
        
        # Extract body (everything after AS)
        body_match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?VIEW\s+[^(]+AS\s+(.+)', 
                              sql, re.IGNORECASE | re.DOTALL)
        body = body_match.group(1) if body_match else sql
        
        return SqlObject(
            object_type=ObjectType.VIEW,
            schema=schema,
            name=name,
            raw_sql=sql,
            normalized_sql=normalized,
            body=body
        )
    
    def _parse_procedure(self, sql: str, normalized: str) -> SqlObject:
        """Parse a CREATE PROCEDURE statement."""
        schema, name = self._extract_schema_name(sql)
        
        # Extract body
        body_match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)\s+[^(]+(?:\([^)]*\))?\s+AS\s+(.+)', 
                              sql, re.IGNORECASE | re.DOTALL)
        body = body_match.group(1) if body_match else sql
        
        return SqlObject(
            object_type=ObjectType.PROCEDURE,
            schema=schema,
            name=name,
            raw_sql=sql,
            normalized_sql=normalized,
            body=body
        )
    
    def _parse_function(self, sql: str, normalized: str) -> SqlObject:
        """Parse a CREATE FUNCTION statement."""
        schema, name = self._extract_schema_name(sql)
        
        # Extract body
        body_match = re.search(r'CREATE\s+(?:OR\s+ALTER\s+)?FUNCTION\s+[^(]+\([^)]*\)\s+RETURNS\s+.+AS\s+(.+)', 
                              sql, re.IGNORECASE | re.DOTALL)
        body = body_match.group(1) if body_match else sql
        
        return SqlObject(
            object_type=ObjectType.FUNCTION,
            schema=schema,
            name=name,
            raw_sql=sql,
            normalized_sql=normalized,
            body=body
        )
    
    def _parse_trigger(self, sql: str, normalized: str) -> SqlObject:
        """Parse a CREATE TRIGGER statement."""
        schema, name = self._extract_schema_name(sql)
        
        return SqlObject(
            object_type=ObjectType.TRIGGER,
            schema=schema,
            name=name,
            raw_sql=sql,
            normalized_sql=normalized,
            body=sql
        )
    
    def _parse_index(self, sql: str, normalized: str) -> SqlObject:
        """Parse a CREATE INDEX statement."""
        # Extract index name and table
        match = re.search(r'CREATE\s+(UNIQUE\s+)?(CLUSTERED\s+|NONCLUSTERED\s+)?INDEX\s+(\S+)\s+ON\s+(\S+)\s*\(([^)]+)\)', 
                         sql, re.IGNORECASE)
        
        if match:
            index_name = match.group(3)
            table_ref = match.group(4)
            
            # Parse table schema/name
            if '.' in table_ref:
                parts = table_ref.replace('[', '').replace(']', '').split('.')
                schema = parts[0]
                name = f"{index_name}_on_{parts[1]}"
            else:
                schema = 'dbo'
                name = f"{index_name}_on_{table_ref.replace('[', '').replace(']', '')}"
        else:
            schema, name = self._extract_schema_name(sql)
        
        return SqlObject(
            object_type=ObjectType.INDEX,
            schema=schema,
            name=name,
            raw_sql=sql,
            normalized_sql=normalized,
            body=sql
        )


# =============================================================================
# TABLE COMPARATOR
# =============================================================================

class TableComparator:
    """Compares table structures and generates differences."""
    
    def compare(self, prod_table: SqlObject, dev_table: SqlObject) -> ObjectDifference:
        """Compare two tables and return difference details."""
        details: Dict[str, Any] = {
            "missing_columns": [],
            "extra_columns": [],
            "modified_columns": [],
            "missing_constraints": [],
            "extra_constraints": [],
            "missing_indexes": [],
            "extra_indexes": []
        }
        
        risk_level = ChangeRisk.SAFE
        manual_review_reasons: List[str] = []
        
        # Compare columns
        prod_cols = set(prod_table.columns.keys())
        dev_cols = set(dev_table.columns.keys())
        
        # Missing columns (in PROD but not DEV) - need to add
        for col_name in prod_cols - dev_cols:
            col = prod_table.columns[col_name]
            details["missing_columns"].append({
                "name": col_name,
                "definition": self._column_to_sql(col)
            })
        
        # Extra columns (in DEV but not PROD) - potential drift
        for col_name in dev_cols - prod_cols:
            col = dev_table.columns[col_name]
            details["extra_columns"].append({
                "name": col_name,
                "data_type": col.data_type,
                "nullable": col.nullable
            })
            risk_level = ChangeRisk.MANUAL_REVIEW
            manual_review_reasons.append(f"Column '{col_name}' exists in DEV but not PROD - manual removal required")
        
        # Modified columns (same name, different definition)
        for col_name in prod_cols & dev_cols:
            prod_col = prod_table.columns[col_name]
            dev_col = dev_table.columns[col_name]
            
            col_diff = self._compare_columns(prod_col, dev_col)
            if col_diff:
                details["modified_columns"].append({
                    "name": col_name,
                    "prod_definition": self._column_to_sql(prod_col),
                    "dev_definition": self._column_to_sql(dev_col),
                    "differences": col_diff
                })
                
                # Check for risky changes
                if "data_type" in col_diff:
                    risk_level = ChangeRisk.MANUAL_REVIEW
                    manual_review_reasons.append(f"Data type change for '{col_name}' requires manual review")
                if "nullable" in col_diff and prod_col.nullable == False:
                    # Adding NOT NULL constraint to existing column is risky
                    risk_level = ChangeRisk.MANUAL_REVIEW
                    manual_review_reasons.append(f"Adding NOT NULL to '{col_name}' requires manual review")
        
        # Compare constraints
        prod_constraints = {self._constraint_key(c): c for c in prod_table.constraints}
        dev_constraints = {self._constraint_key(c): c for c in dev_table.constraints}
        
        for key in set(prod_constraints.keys()) - set(dev_constraints.keys()):
            details["missing_constraints"].append({
                "name": prod_constraints[key].name,
                "type": prod_constraints[key].constraint_type,
                "sql": self._constraint_to_sql(prod_constraints[key], prod_table.full_name)
            })
        
        for key in set(dev_constraints.keys()) - set(prod_constraints.keys()):
            details["extra_constraints"].append({
                "name": dev_constraints[key].name,
                "type": dev_constraints[key].constraint_type
            })
        
        # Compare indexes
        prod_indexes = {idx.name.lower(): idx for idx in prod_table.indexes}
        dev_indexes = {idx.name.lower(): idx for idx in dev_table.indexes}
        
        for name in set(prod_indexes.keys()) - set(dev_indexes.keys()):
            details["missing_indexes"].append({
                "name": prod_indexes[name].name,
                "sql": prod_table.indexes[list(prod_indexes.keys()).index(name)].raw_sql if prod_table.indexes else ""
            })
        
        for name in set(dev_indexes.keys()) - set(prod_indexes.keys()):
            details["extra_indexes"].append({"name": name})
        
        # Check if there are any actual differences
        has_differences = (
            details["missing_columns"] or
            details["extra_columns"] or
            details["modified_columns"] or
            details["missing_constraints"] or
            details["extra_constraints"] or
            details["missing_indexes"] or
            details["extra_indexes"]
        )
        
        if not has_differences:
            return None
        
        return ObjectDifference(
            difference_type=DifferenceType.MODIFIED,
            object_type=ObjectType.TABLE,
            schema=prod_table.schema,
            name=prod_table.name,
            prod_object=prod_table,
            dev_object=dev_table,
            details=details,
            risk_level=risk_level,
            manual_review_reason="; ".join(manual_review_reasons) if manual_review_reasons else None
        )
    
    def _compare_columns(self, prod: ColumnDefinition, dev: ColumnDefinition) -> List[str]:
        """Compare two column definitions and return list of differences."""
        differences = []
        
        if prod.data_type.lower() != dev.data_type.lower():
            differences.append("data_type")
        if prod.length != dev.length:
            differences.append("length")
        if prod.nullable != dev.nullable:
            differences.append("nullable")
        if prod.default_value != dev.default_value:
            differences.append("default_value")
        if prod.is_identity != dev.is_identity:
            differences.append("identity")
        if prod.is_computed != dev.is_computed:
            differences.append("computed")
        
        return differences
    
    def _constraint_key(self, constraint: ConstraintDefinition) -> str:
        """Generate a key for constraint comparison."""
        cols = ','.join(sorted(constraint.columns))
        return f"{constraint.constraint_type}:{cols}"
    
    def _column_to_sql(self, col: ColumnDefinition) -> str:
        """Convert column definition to SQL fragment."""
        parts = [f"[{col.name}]", col.data_type]
        if col.length:
            parts.append(f"({col.length})")
        if col.is_computed and col.computed_expression:
            parts.append(f"AS {col.computed_expression}")
        if col.is_identity:
            parts.append("IDENTITY")
            if col.identity_seed is not None and col.identity_increment is not None:
                parts.append(f"({col.identity_seed},{col.identity_increment})")
        if not col.nullable:
            parts.append("NOT NULL")
        else:
            parts.append("NULL")
        if col.default_value:
            parts.append(f"DEFAULT {col.default_value}")
        return ' '.join(parts)
    
    def _constraint_to_sql(self, constraint: ConstraintDefinition, table_name: str) -> str:
        """Convert constraint to ALTER TABLE SQL."""
        constraint_name = f"[{constraint.name}]" if constraint.name else ""
        
        if constraint.constraint_type == 'PRIMARY KEY':
            cols = ','.join(f"[{c}]" for c in constraint.columns)
            if constraint_name:
                return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({cols})"
            return f"ALTER TABLE {table_name} ADD PRIMARY KEY ({cols})"
        
        elif constraint.constraint_type == 'UNIQUE':
            cols = ','.join(f"[{c}]" for c in constraint.columns)
            if constraint_name:
                return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({cols})"
            return f"ALTER TABLE {table_name} ADD UNIQUE ({cols})"
        
        elif constraint.constraint_type == 'FOREIGN KEY':
            cols = ','.join(f"[{c}]" for c in constraint.columns)
            ref_cols = ','.join(f"[{c}]" for c in constraint.reference_columns)
            if constraint_name:
                return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({cols}) REFERENCES {constraint.reference_table} ({ref_cols})"
            return f"ALTER TABLE {table_name} ADD FOREIGN KEY ({cols}) REFERENCES {constraint.reference_table} ({ref_cols})"
        
        elif constraint.constraint_type == 'CHECK':
            if constraint_name:
                return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} CHECK ({constraint.check_expression})"
            return f"ALTER TABLE {table_name} ADD CHECK ({constraint.check_expression})"
        
        return f"-- Unknown constraint type: {constraint.constraint_type}"


# =============================================================================
# SQL GENERATOR
# =============================================================================

class SqlGenerator:
    """Generates SQL to update DEV to match PROD."""
    
    def __init__(self, include_drops: bool = False):
        self.include_drops = include_drops
    
    def generate(self, result: ComparisonResult, prod_file: str, dev_file: str) -> str:
        """Generate complete differences.sql content."""
        lines: List[str] = []
        
        # Header summary
        lines.extend(self._generate_header(result, prod_file, dev_file))
        lines.append("")
        
        # Manual review section first
        manual_review = result.get_manual_review_items()
        if manual_review:
            lines.append("-- =========================================================")
            lines.append("-- MANUAL REVIEW REQUIRED")
            lines.append("-- The following changes require manual review before execution:")
            lines.append("-- =========================================================")
            for item in manual_review:
                lines.append(f"-- [{item.object_type.value}] {item.schema}.{item.name}")
                if item.manual_review_reason:
                    lines.append(f"--   Reason: {item.manual_review_reason}")
            lines.append("")
            lines.append("GO")
            lines.append("")
        
        # Generate SQL by object type in dependency order
        lines.extend(self._generate_tables(result))
        lines.extend(self._generate_constraints(result))
        lines.extend(self._generate_indexes(result))
        lines.extend(self._generate_functions(result))
        lines.extend(self._generate_views(result))
        lines.extend(self._generate_procedures(result))
        lines.extend(self._generate_triggers(result))
        
        # Drop statements (if enabled)
        if self.include_drops:
            lines.extend(self._generate_drops(result))
        
        return '\n'.join(lines)
    
    def _clean_sql_for_output(self, sql: str) -> str:
        """Remove leading comment headers from SQL for clean output."""
        lines = sql.split('\n')
        result = []
        in_header = True
        
        for line in lines:
            # Skip comment lines and empty lines at the beginning
            if in_header:
                stripped = line.strip()
                if stripped.startswith('--') or stripped.startswith('/*') or stripped.startswith('*') or not stripped:
                    continue
                else:
                    in_header = False
            result.append(line)
        
        return '\n'.join(result).strip()
    
    def _generate_header(self, result: ComparisonResult, prod_file: str, dev_file: str) -> List[str]:
        """Generate the summary header."""
        lines = []
        counts = result.summary_counts()
        
        lines.append("-- =========================================================")
        lines.append("-- SCHEMA DIFFERENCE SUMMARY")
        lines.append(f"-- Source of truth: PROD")
        lines.append(f"-- Target to update: DEV")
        lines.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"-- Source file: {prod_file}")
        lines.append(f"-- Target file: {dev_file}")
        lines.append("-- =========================================================")
        lines.append("-- Summary counts:")
        lines.append(f"--   Objects only in PROD: {counts['objects_only_in_prod']}")
        lines.append(f"--   Objects only in DEV: {counts['objects_only_in_dev']}")
        lines.append(f"--   Modified tables: {counts['modified_tables']}")
        lines.append(f"--   Modified views: {counts['modified_views']}")
        lines.append(f"--   Modified procedures: {counts['modified_procedures']}")
        lines.append(f"--   Modified functions: {counts['modified_functions']}")
        lines.append(f"--   Manual review items: {counts['manual_review_items']}")
        lines.append("-- =========================================================")
        
        # List objects only in PROD
        prod_only = result.get_by_type(DifferenceType.ONLY_IN_PROD)
        if prod_only:
            lines.append("-- Objects only in PROD:")
            for item in sorted(prod_only, key=lambda x: (x.object_type.value, x.schema, x.name)):
                lines.append(f"--   {item.object_type.value} {item.schema}.{item.name}")
            lines.append("-- =========================================================")
        
        # List objects only in DEV
        dev_only = result.get_by_type(DifferenceType.ONLY_IN_DEV)
        if dev_only:
            lines.append("-- Objects only in DEV (drift - not automatically dropped):")
            for item in sorted(dev_only, key=lambda x: (x.object_type.value, x.schema, x.name)):
                lines.append(f"--   {item.object_type.value} {item.schema}.{item.name}")
            lines.append("-- =========================================================")
        
        return lines
    
    def _generate_tables(self, result: ComparisonResult) -> List[str]:
        """Generate SQL for table differences."""
        lines = []
        
        # New tables from PROD
        new_tables = [d for d in result.get_by_type(DifferenceType.ONLY_IN_PROD) 
                     if d.object_type == ObjectType.TABLE]
        
        if new_tables:
            lines.append("-- =========================================================")
            lines.append("-- NEW TABLES (from PROD)")
            lines.append("-- =========================================================")
            for diff in new_tables:
                if diff.prod_object:
                    lines.append("")
                    lines.append(f"-- Create table {diff.schema}.{diff.name}")
                    lines.append(self._clean_sql_for_output(diff.prod_object.raw_sql))
                    lines.append("")
                    lines.append("GO")
        
        # Modified tables
        modified_tables = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                          if d.object_type == ObjectType.TABLE]
        
        if modified_tables:
            lines.append("")
            lines.append("-- =========================================================")
            lines.append("-- MODIFIED TABLES")
            lines.append("-- =========================================================")
            
            for diff in modified_tables:
                lines.extend(self._generate_table_modifications(diff))
        
        return lines
    
    def _generate_table_modifications(self, diff: ObjectDifference) -> List[str]:
        """Generate ALTER TABLE statements for table modifications."""
        lines = []
        table_name = f"[{diff.schema}].[{diff.name}]"
        
        lines.append("")
        lines.append(f"-- Modifications for table {diff.schema}.{diff.name}")
        
        if diff.risk_level == ChangeRisk.MANUAL_REVIEW:
            lines.append(f"-- MANUAL REVIEW REQUIRED: {diff.manual_review_reason}")
        
        # Add missing columns
        for col_info in diff.details.get("missing_columns", []):
            col_sql = col_info["definition"]
            lines.append(f"ALTER TABLE {table_name} ADD {col_sql};")
        
        # Add missing constraints
        for constraint_info in diff.details.get("missing_constraints", []):
            lines.append(f"{constraint_info['sql']};")
        
        # Add missing indexes
        for idx_info in diff.details.get("missing_indexes", []):
            if idx_info.get("sql"):
                lines.append(idx_info["sql"])
        
        lines.append("")
        lines.append("GO")
        
        return lines
    
    def _generate_constraints(self, result: ComparisonResult) -> List[str]:
        """Generate SQL for standalone constraint statements."""
        lines = []
        # Most constraints are handled inline with tables
        return lines
    
    def _generate_indexes(self, result: ComparisonResult) -> List[str]:
        """Generate SQL for standalone index statements."""
        lines = []
        
        new_indexes = [d for d in result.get_by_type(DifferenceType.ONLY_IN_PROD) 
                      if d.object_type == ObjectType.INDEX]
        
        if new_indexes:
            lines.append("")
            lines.append("-- =========================================================")
            lines.append("-- NEW INDEXES (from PROD)")
            lines.append("-- =========================================================")
            
            for diff in new_indexes:
                if diff.prod_object:
                    lines.append("")
                    lines.append(self._clean_sql_for_output(diff.prod_object.raw_sql))
                    lines.append("")
                    lines.append("GO")
        
        return lines
    
    def _generate_views(self, result: ComparisonResult) -> List[str]:
        """Generate SQL for view differences."""
        lines = []
        
        # New views
        new_views = [d for d in result.get_by_type(DifferenceType.ONLY_IN_PROD) 
                    if d.object_type == ObjectType.VIEW]
        
        # Modified views
        modified_views = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                         if d.object_type == ObjectType.VIEW]
        
        if new_views or modified_views:
            lines.append("")
            lines.append("-- =========================================================")
            lines.append("-- VIEWS")
            lines.append("-- =========================================================")
            
            for diff in new_views + modified_views:
                if diff.prod_object:
                    lines.append("")
                    lines.append(f"-- View: {diff.schema}.{diff.name}")
                    
                    # Start with raw SQL, cleaned of headers
                    sql = self._clean_sql_for_output(diff.prod_object.raw_sql)
                    
                    if diff.difference_type == DifferenceType.MODIFIED:
                        # Use CREATE OR ALTER if available, otherwise DROP/CREATE
                        if 'CREATE OR ALTER' not in sql.upper():
                            lines.append(f"IF OBJECT_ID('{diff.schema}.{diff.name}', 'V') IS NOT NULL")
                            lines.append(f"    DROP VIEW [{diff.schema}].[{diff.name}];")
                            lines.append("GO")
                            sql = re.sub(r'CREATE\s+VIEW', 'CREATE VIEW', sql, flags=re.IGNORECASE)
                    
                    lines.append(sql)
                    lines.append("")
                    lines.append("GO")
        
        return lines
    
    def _generate_procedures(self, result: ComparisonResult) -> List[str]:
        """Generate SQL for procedure differences."""
        lines = []
        
        new_procs = [d for d in result.get_by_type(DifferenceType.ONLY_IN_PROD) 
                    if d.object_type == ObjectType.PROCEDURE]
        
        modified_procs = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                         if d.object_type == ObjectType.PROCEDURE]
        
        if new_procs or modified_procs:
            lines.append("")
            lines.append("-- =========================================================")
            lines.append("-- STORED PROCEDURES")
            lines.append("-- =========================================================")
            
            for diff in new_procs + modified_procs:
                if diff.prod_object:
                    lines.append("")
                    lines.append(f"-- Procedure: {diff.schema}.{diff.name}")
                    
                    # Start with raw SQL, cleaned of headers
                    sql = self._clean_sql_for_output(diff.prod_object.raw_sql)
                    
                    if diff.difference_type == DifferenceType.MODIFIED:
                        if 'CREATE OR ALTER' not in sql.upper():
                            lines.append(f"IF OBJECT_ID('{diff.schema}.{diff.name}', 'P') IS NOT NULL")
                            lines.append(f"    DROP PROCEDURE [{diff.schema}].[{diff.name}];")
                            lines.append("GO")
                            sql = re.sub(r'CREATE\s+(?:OR\s+ALTER\s+)?(?:PROCEDURE|PROC)', 
                                        'CREATE PROCEDURE', sql, flags=re.IGNORECASE)
                    
                    lines.append(sql)
                    lines.append("")
                    lines.append("GO")
        
        return lines
    
    def _generate_functions(self, result: ComparisonResult) -> List[str]:
        """Generate SQL for function differences."""
        lines = []
        
        new_funcs = [d for d in result.get_by_type(DifferenceType.ONLY_IN_PROD) 
                    if d.object_type == ObjectType.FUNCTION]
        
        modified_funcs = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                         if d.object_type == ObjectType.FUNCTION]
        
        if new_funcs or modified_funcs:
            lines.append("")
            lines.append("-- =========================================================")
            lines.append("-- FUNCTIONS")
            lines.append("-- =========================================================")
            
            for diff in new_funcs + modified_funcs:
                if diff.prod_object:
                    lines.append("")
                    lines.append(f"-- Function: {diff.schema}.{diff.name}")
                    
                    # Start with raw SQL, cleaned of headers
                    sql = self._clean_sql_for_output(diff.prod_object.raw_sql)
                    
                    if diff.difference_type == DifferenceType.MODIFIED:
                        if 'CREATE OR ALTER' not in sql.upper():
                            lines.append(f"IF OBJECT_ID('{diff.schema}.{diff.name}', 'FN') IS NOT NULL")
                            lines.append(f"    DROP FUNCTION [{diff.schema}].[{diff.name}];")
                            lines.append("GO")
                            sql = re.sub(r'CREATE\s+(?:OR\s+ALTER\s+)?FUNCTION', 
                                        'CREATE FUNCTION', sql, flags=re.IGNORECASE)
                    
                    lines.append(sql)
                    lines.append("")
                    lines.append("GO")
        
        return lines
    
    def _generate_triggers(self, result: ComparisonResult) -> List[str]:
        """Generate SQL for trigger differences."""
        lines = []
        
        new_triggers = [d for d in result.get_by_type(DifferenceType.ONLY_IN_PROD) 
                       if d.object_type == ObjectType.TRIGGER]
        
        modified_triggers = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                            if d.object_type == ObjectType.TRIGGER]
        
        if new_triggers or modified_triggers:
            lines.append("")
            lines.append("-- =========================================================")
            lines.append("-- TRIGGERS")
            lines.append("-- =========================================================")
            
            for diff in new_triggers + modified_triggers:
                if diff.prod_object:
                    lines.append("")
                    lines.append(f"-- Trigger: {diff.schema}.{diff.name}")
                    
                    # Start with raw SQL, cleaned of headers
                    sql = self._clean_sql_for_output(diff.prod_object.raw_sql)
                    
                    if diff.difference_type == DifferenceType.MODIFIED:
                        if 'CREATE OR ALTER' not in sql.upper():
                            lines.append(f"IF OBJECT_ID('{diff.schema}.{diff.name}', 'TR') IS NOT NULL")
                            lines.append(f"    DROP TRIGGER [{diff.schema}].[{diff.name}];")
                            lines.append("GO")
                            sql = re.sub(r'CREATE\s+(?:OR\s+ALTER\s+)?TRIGGER', 
                                        'CREATE TRIGGER', sql, flags=re.IGNORECASE)
                    
                    lines.append(sql)
                    lines.append("")
                    lines.append("GO")
        
        return lines
    
    def _generate_drops(self, result: ComparisonResult) -> List[str]:
        """Generate DROP statements for DEV-only objects."""
        lines = []
        
        dev_only = result.get_by_type(DifferenceType.ONLY_IN_DEV)
        
        if dev_only:
            lines.append("")
            lines.append("-- =========================================================")
            lines.append("-- DROP STATEMENTS (DEV-only objects)")
            lines.append("-- WARNING: Review carefully before executing!")
            lines.append("-- =========================================================")
            
            for diff in sorted(dev_only, key=lambda x: (x.object_type.value, x.schema, x.name)):
                lines.append("")
                lines.append(f"-- Drop {diff.object_type.value}: {diff.schema}.{diff.name}")
                
                obj_type_code = {
                    ObjectType.TABLE: 'U',
                    ObjectType.VIEW: 'V',
                    ObjectType.PROCEDURE: 'P',
                    ObjectType.FUNCTION: 'FN',
                    ObjectType.TRIGGER: 'TR',
                    ObjectType.INDEX: 'IX'
                }.get(diff.object_type, 'U')
                
                if diff.object_type == ObjectType.INDEX:
                    # Index drop syntax is different
                    lines.append(f"-- DROP INDEX [{diff.name}] ON [{diff.schema}].[table_name];")
                else:
                    type_name = diff.object_type.value
                    if diff.object_type == ObjectType.PROCEDURE:
                        type_name = "PROCEDURE"
                    lines.append(f"IF OBJECT_ID('{diff.schema}.{diff.name}', '{obj_type_code}') IS NOT NULL")
                    lines.append(f"    DROP {type_name} [{diff.schema}].[{diff.name}];")
                
                lines.append("GO")
        
        return lines


# =============================================================================
# REPORT GENERATORS
# =============================================================================

class JsonReportGenerator:
    """Generates JSON report of schema differences."""
    
    def generate(self, result: ComparisonResult, prod_file: str, dev_file: str) -> str:
        """Generate JSON report content."""
        report = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "source_file": prod_file,
                "target_file": dev_file,
                "source_of_truth": "PROD",
                "target": "DEV"
            },
            "summary": result.summary_counts(),
            "objects_only_in_prod": [
                d.to_dict() for d in sorted(
                    result.get_by_type(DifferenceType.ONLY_IN_PROD),
                    key=lambda x: (x.object_type.value, x.schema, x.name)
                )
            ],
            "objects_only_in_dev": [
                d.to_dict() for d in sorted(
                    result.get_by_type(DifferenceType.ONLY_IN_DEV),
                    key=lambda x: (x.object_type.value, x.schema, x.name)
                )
            ],
            "modified_objects": [
                d.to_dict() for d in sorted(
                    result.get_by_type(DifferenceType.MODIFIED),
                    key=lambda x: (x.object_type.value, x.schema, x.name)
                )
            ],
            "manual_review_items": [
                d.to_dict() for d in sorted(
                    result.get_manual_review_items(),
                    key=lambda x: (x.object_type.value, x.schema, x.name)
                )
            ]
        }
        
        return json.dumps(report, indent=2, default=str)


class MarkdownReportGenerator:
    """Generates Markdown report of schema differences."""
    
    def generate(self, result: ComparisonResult, prod_file: str, dev_file: str) -> str:
        """Generate Markdown report content."""
        lines = []
        counts = result.summary_counts()
        
        lines.append("# Schema Difference Report")
        lines.append("")
        lines.append("## Metadata")
        lines.append("")
        lines.append(f"- **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"- **Source (PROD):** `{prod_file}`")
        lines.append(f"- **Target (DEV):** `{dev_file}`")
        lines.append("")
        
        lines.append("## Summary")
        lines.append("")
        lines.append(f"| Metric | Count |")
        lines.append(f"|--------|-------|")
        lines.append(f"| Objects only in PROD | {counts['objects_only_in_prod']} |")
        lines.append(f"| Objects only in DEV | {counts['objects_only_in_dev']} |")
        lines.append(f"| Modified tables | {counts['modified_tables']} |")
        lines.append(f"| Modified views | {counts['modified_views']} |")
        lines.append(f"| Modified procedures | {counts['modified_procedures']} |")
        lines.append(f"| Modified functions | {counts['modified_functions']} |")
        lines.append(f"| Manual review items | {counts['manual_review_items']} |")
        lines.append("")
        
        # Manual review section
        manual_review = result.get_manual_review_items()
        if manual_review:
            lines.append("## ⚠️ Manual Review Required")
            lines.append("")
            for item in manual_review:
                lines.append(f"### {item.object_type.value}: {item.schema}.{item.name}")
                lines.append("")
                lines.append(f"**Reason:** {item.manual_review_reason}")
                lines.append("")
        
        # Objects only in PROD
        prod_only = result.get_by_type(DifferenceType.ONLY_IN_PROD)
        if prod_only:
            lines.append("## Objects Only in PROD (Need to be Added)")
            lines.append("")
            lines.append("| Type | Schema | Name |")
            lines.append("|------|--------|------|")
            for item in sorted(prod_only, key=lambda x: (x.object_type.value, x.schema, x.name)):
                lines.append(f"| {item.object_type.value} | {item.schema} | {item.name} |")
            lines.append("")
        
        # Objects only in DEV
        dev_only = result.get_by_type(DifferenceType.ONLY_IN_DEV)
        if dev_only:
            lines.append("## Objects Only in DEV (Potential Drift)")
            lines.append("")
            lines.append("| Type | Schema | Name |")
            lines.append("|------|--------|------|")
            for item in sorted(dev_only, key=lambda x: (x.object_type.value, x.schema, x.name)):
                lines.append(f"| {item.object_type.value} | {item.schema} | {item.name} |")
            lines.append("")
        
        # Modified tables
        modified_tables = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                          if d.object_type == ObjectType.TABLE]
        if modified_tables:
            lines.append("## Modified Tables")
            lines.append("")
            for diff in modified_tables:
                lines.append(f"### {diff.schema}.{diff.name}")
                lines.append("")
                
                if diff.risk_level == ChangeRisk.MANUAL_REVIEW:
                    lines.append(f"**⚠️ MANUAL REVIEW:** {diff.manual_review_reason}")
                    lines.append("")
                
                if diff.details.get("missing_columns"):
                    lines.append("**Missing Columns (need to add):**")
                    for col in diff.details["missing_columns"]:
                        lines.append(f"- `{col['name']}`: `{col['definition']}`")
                    lines.append("")
                
                if diff.details.get("extra_columns"):
                    lines.append("**Extra Columns (in DEV only):**")
                    for col in diff.details["extra_columns"]:
                        lines.append(f"- `{col['name']}` ({col['data_type']})")
                    lines.append("")
                
                if diff.details.get("modified_columns"):
                    lines.append("**Modified Columns:**")
                    for col in diff.details["modified_columns"]:
                        lines.append(f"- `{col['name']}`: {', '.join(col['differences'])}")
                    lines.append("")
                
                if diff.details.get("missing_constraints"):
                    lines.append("**Missing Constraints:**")
                    for constraint in diff.details["missing_constraints"]:
                        lines.append(f"- `{constraint['name']}` ({constraint['type']})")
                    lines.append("")
        
        # Modified views
        modified_views = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                         if d.object_type == ObjectType.VIEW]
        if modified_views:
            lines.append("## Modified Views")
            lines.append("")
            for diff in modified_views:
                lines.append(f"- `{diff.schema}.{diff.name}`")
            lines.append("")
        
        # Modified procedures
        modified_procs = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                         if d.object_type == ObjectType.PROCEDURE]
        if modified_procs:
            lines.append("## Modified Procedures")
            lines.append("")
            for diff in modified_procs:
                lines.append(f"- `{diff.schema}.{diff.name}`")
            lines.append("")
        
        # Modified functions
        modified_funcs = [d for d in result.get_by_type(DifferenceType.MODIFIED) 
                         if d.object_type == ObjectType.FUNCTION]
        if modified_funcs:
            lines.append("## Modified Functions")
            lines.append("")
            for diff in modified_funcs:
                lines.append(f"- `{diff.schema}.{diff.name}`")
            lines.append("")
        
        return '\n'.join(lines)


# =============================================================================
# MAIN COMPARATOR
# =============================================================================

class SchemaComparator:
    """Main class for comparing SQL schemas."""
    
    def __init__(self, ignore_comments: bool = True, ignore_case: bool = True, 
                 debug: bool = False, strict: bool = False):
        self.normalizer = SqlNormalizer(ignore_comments=ignore_comments, ignore_case=ignore_case)
        self.parser = SqlParser(self.normalizer, debug=debug)
        self.table_comparator = TableComparator()
        self.debug = debug
        self.strict = strict
    
    def compare(self, prod_path: Path, dev_path: Path) -> ComparisonResult:
        """Compare two SQL schema files."""
        result = ComparisonResult()
        
        # Parse both files
        if self.debug:
            print(f"Parsing PROD file: {prod_path}")
        
        result.prod_objects = self.parser.parse_file(prod_path)
        result.parse_errors.extend(self.parser.errors)
        
        if self.debug:
            print(f"  Found {len(result.prod_objects)} objects")
            print(f"Parsing DEV file: {dev_path}")
        
        result.dev_objects = self.parser.parse_file(dev_path)
        result.parse_errors.extend(self.parser.errors)
        
        if self.debug:
            print(f"  Found {len(result.dev_objects)} objects")
        
        # In strict mode, parsing errors are fatal
        if self.strict and result.parse_errors:
            return result
        
        # Find differences
        result.differences = self._find_differences(result)
        
        return result
    
    def _find_differences(self, result: ComparisonResult) -> List[ObjectDifference]:
        """Find all differences between PROD and DEV."""
        differences: List[ObjectDifference] = []
        
        prod_ids = set(result.prod_objects.keys())
        dev_ids = set(result.dev_objects.keys())
        
        # Objects only in PROD
        for obj_id in prod_ids - dev_ids:
            obj = result.prod_objects[obj_id]
            differences.append(ObjectDifference(
                difference_type=DifferenceType.ONLY_IN_PROD,
                object_type=obj.object_type,
                schema=obj.schema,
                name=obj.name,
                prod_object=obj,
                risk_level=ChangeRisk.SAFE
            ))
        
        # Objects only in DEV
        for obj_id in dev_ids - prod_ids:
            obj = result.dev_objects[obj_id]
            differences.append(ObjectDifference(
                difference_type=DifferenceType.ONLY_IN_DEV,
                object_type=obj.object_type,
                schema=obj.schema,
                name=obj.name,
                dev_object=obj,
                risk_level=ChangeRisk.CAUTION
            ))
        
        # Objects in both - compare
        for obj_id in prod_ids & dev_ids:
            prod_obj = result.prod_objects[obj_id]
            dev_obj = result.dev_objects[obj_id]
            
            diff = self._compare_objects(prod_obj, dev_obj)
            if diff:
                differences.append(diff)
        
        return differences
    
    def _compare_objects(self, prod: SqlObject, dev: SqlObject) -> Optional[ObjectDifference]:
        """Compare two objects of the same identity."""
        if prod.object_type == ObjectType.TABLE:
            return self._compare_tables(prod, dev)
        else:
            return self._compare_code_objects(prod, dev)
    
    def _compare_tables(self, prod: SqlObject, dev: SqlObject) -> Optional[ObjectDifference]:
        """Compare two table objects."""
        return self.table_comparator.compare(prod, dev)
    
    def _compare_code_objects(self, prod: SqlObject, dev: SqlObject) -> Optional[ObjectDifference]:
        """Compare view/procedure/function objects by normalized body."""
        if prod.normalized_sql == dev.normalized_sql:
            return None
        
        return ObjectDifference(
            difference_type=DifferenceType.MODIFIED,
            object_type=prod.object_type,
            schema=prod.schema,
            name=prod.name,
            prod_object=prod,
            dev_object=dev,
            details={
                "prod_body_preview": prod.body[:200] if prod.body else None,
                "dev_body_preview": dev.body[:200] if dev.body else None
            },
            risk_level=ChangeRisk.SAFE
        )


# =============================================================================
# CLI
# =============================================================================

def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Compare SQL schema files and generate update scripts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --prod prod.sql --dev dev.sql
  %(prog)s --prod prod.sql --dev dev.sql --out my_diff.sql --include-drops
  %(prog)s --prod prod.sql --dev dev.sql --report-json diff.json --report-md diff.md

Exit Codes:
  0 - No differences found
  1 - Differences found and files generated
  2 - Parse error or fatal error
        """
    )
    
    parser.add_argument(
        '--prod', '-p',
        required=True,
        help='Path to PROD SQL file (source of truth)'
    )
    
    parser.add_argument(
        '--dev', '-d',
        required=True,
        help='Path to DEV SQL file (target to update)'
    )
    
    parser.add_argument(
        '--out', '-o',
        default='differences.sql',
        help='Output SQL file path (default: differences.sql)'
    )
    
    parser.add_argument(
        '--report-json',
        default='schema_diff_report.json',
        help='JSON report file path (default: schema_diff_report.json)'
    )
    
    parser.add_argument(
        '--report-md',
        default='schema_diff_report.md',
        help='Markdown report file path (default: schema_diff_report.md)'
    )
    
    parser.add_argument(
        '--include-drops',
        action='store_true',
        help='Include DROP statements for DEV-only objects'
    )
    
    parser.add_argument(
        '--ignore-comments',
        action='store_true',
        default=True,
        help='Ignore comments when comparing (default: True)'
    )
    
    parser.add_argument(
        '--ignore-case',
        action='store_true',
        default=True,
        help='Ignore case when comparing (default: True)'
    )
    
    parser.add_argument(
        '--strict',
        action='store_true',
        help='Treat parse warnings as fatal errors'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug output'
    )
    
    parser.add_argument(
        '--version', '-v',
        action='version',
        version=f'%(prog)s {VERSION}'
    )
    
    return parser


def main() -> int:
    """Main entry point."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    prod_path = Path(args.prod)
    dev_path = Path(args.dev)
    out_path = Path(args.out)
    json_path = Path(args.report_json)
    md_path = Path(args.report_md)
    
    # Validate input files
    if not prod_path.exists():
        print(f"Error: PROD file not found: {prod_path}", file=sys.stderr)
        return 2
    
    if not dev_path.exists():
        print(f"Error: DEV file not found: {dev_path}", file=sys.stderr)
        return 2
    
    # Create comparator
    comparator = SchemaComparator(
        ignore_comments=args.ignore_comments,
        ignore_case=args.ignore_case,
        debug=args.debug,
        strict=args.strict
    )
    
    if args.debug:
        print(f"Comparing schemas...")
        print(f"  PROD: {prod_path.absolute()}")
        print(f"  DEV: {dev_path.absolute()}")
    
    # Perform comparison
    try:
        result = comparator.compare(prod_path, dev_path)
    except Exception as e:
        print(f"Error during comparison: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        return 2
    
    # Handle parse errors
    if result.parse_errors:
        print("\nParse warnings/errors:", file=sys.stderr)
        for error in result.parse_errors:
            print(f"  - {error}", file=sys.stderr)
        
        if args.strict:
            return 2
    
    # Print summary to console
    counts = result.summary_counts()
    print("\n" + "=" * 60)
    print("SCHEMA COMPARISON SUMMARY")
    print("=" * 60)
    print(f"Objects parsed from PROD: {len(result.prod_objects)}")
    print(f"Objects parsed from DEV:  {len(result.dev_objects)}")
    print(f"Objects only in PROD:     {counts['objects_only_in_prod']}")
    print(f"Objects only in DEV:      {counts['objects_only_in_dev']}")
    print(f"Modified tables:          {counts['modified_tables']}")
    print(f"Modified views:           {counts['modified_views']}")
    print(f"Modified procedures:      {counts['modified_procedures']}")
    print(f"Modified functions:       {counts['modified_functions']}")
    print(f"Manual review items:      {counts['manual_review_items']}")
    print("=" * 60)
    
    # Generate outputs
    if result.has_differences:
        # Generate SQL differences file
        sql_generator = SqlGenerator(include_drops=args.include_drops)
        differences_sql = sql_generator.generate(
            result, 
            str(prod_path), 
            str(dev_path)
        )
        
        try:
            out_path.write_text(differences_sql, encoding=DEFAULT_ENCODING)
            print(f"\nGenerated SQL file: {out_path.absolute()}")
        except Exception as e:
            print(f"Error writing SQL file: {e}", file=sys.stderr)
            return 2
        
        # Generate JSON report
        json_generator = JsonReportGenerator()
        json_content = json_generator.generate(result, str(prod_path), str(dev_path))
        
        try:
            json_path.write_text(json_content, encoding=DEFAULT_ENCODING)
            print(f"Generated JSON report: {json_path.absolute()}")
        except Exception as e:
            print(f"Error writing JSON report: {e}", file=sys.stderr)
        
        # Generate Markdown report
        md_generator = MarkdownReportGenerator()
        md_content = md_generator.generate(result, str(prod_path), str(dev_path))
        
        try:
            md_path.write_text(md_content, encoding=DEFAULT_ENCODING)
            print(f"Generated Markdown report: {md_path.absolute()}")
        except Exception as e:
            print(f"Error writing Markdown report: {e}", file=sys.stderr)
        
        # Print manual review items
        manual_review = result.get_manual_review_items()
        if manual_review:
            print("\n⚠️  MANUAL REVIEW REQUIRED:")
            for item in manual_review:
                print(f"   - [{item.object_type.value}] {item.schema}.{item.name}")
                if item.manual_review_reason:
                    print(f"     Reason: {item.manual_review_reason}")
        
        return 1
    else:
        print("\n✓ No differences found. Schema files are identical.")
        
        # Still generate empty reports for consistency
        json_generator = JsonReportGenerator()
        json_content = json_generator.generate(result, str(prod_path), str(dev_path))
        
        try:
            json_path.write_text(json_content, encoding=DEFAULT_ENCODING)
            md_path.write_text("# Schema Difference Report\n\nNo differences found.", 
                             encoding=DEFAULT_ENCODING)
        except Exception:
            pass
        
        return 0


if __name__ == '__main__':
    sys.exit(main())
