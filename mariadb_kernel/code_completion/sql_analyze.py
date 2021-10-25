from .sql_fetch import SqlFetch
from typing import Generator, List, Tuple

from re import compile, escape
from collections import Counter
from prompt_toolkit.completion import Completer, Completion
from .completion_engine import suggest_type
from mycli.packages.parseutils import last_word
from mycli.packages.filepaths import parse_path, complete_path, suggest_path
from mycli.packages.special.favoritequeries import FavoriteQueries

default_keywords = [
    "&&",
    "<=",
    "<>",
    "!=",
    ">=",
    "<<",
    ">>",
    "<=>",
    "ACCESSIBLE",
    "ACCOUNT",
    "ACTION",
    "ADD",
    "ADMIN",
    "AFTER",
    "AGAINST",
    "AGGREGATE",
    "ALL",
    "ALGORITHM",
    "ALTER",
    "ALWAYS",
    "ANALYZE",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "ASCII",
    "ASENSITIVE",
    "AT",
    "ATOMIC",
    "AUTHORS",
    "AUTO_INCREMENT",
    "AUTOEXTEND_SIZE",
    "AUTO",
    "AVG",
    "AVG_ROW_LENGTH",
    "BACKUP",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BINLOG",
    "BIT",
    "BLOB",
    "BLOCK",
    "BODY",
    "BOOL",
    "BOOLEAN",
    "BOTH",
    "BTREE",
    "BY",
    "BYTE",
    "CACHE",
    "CALL",
    "CASCADE",
    "CASCADED",
    "CASE",
    "CATALOG_NAME",
    "CHAIN",
    "CHANGE",
    "CHANGED",
    "CHAR",
    "CHARACTER",
    "CHARSET",
    "CHECK",
    "CHECKPOINT",
    "CHECKSUM",
    "CIPHER",
    "CLASS_ORIGIN",
    "CLIENT",
    "CLOB",
    "CLOSE",
    "COALESCE",
    "CODE",
    "COLLATE",
    "COLLATION",
    "COLUMN",
    "COLUMN_NAME",
    "COLUMNS",
    "COLUMN_ADD",
    "COLUMN_CHECK",
    "COLUMN_CREATE",
    "COLUMN_DELETE",
    "COLUMN_GET",
    "COMMENT",
    "COMMIT",
    "COMMITTED",
    "COMPACT",
    "COMPLETION",
    "COMPRESSED",
    "CONCURRENT",
    "CONDITION",
    "CONNECTION",
    "CONSISTENT",
    "CONSTRAINT",
    "CONSTRAINT_CATALOG",
    "CONSTRAINT_NAME",
    "CONSTRAINT_SCHEMA",
    "CONTAINS",
    "CONTEXT",
    "CONTINUE",
    "CONTRIBUTORS",
    "CONVERT",
    "CPU",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_POS",
    "CURRENT_ROLE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "CURSOR_NAME",
    "CYCLE",
    "DATA",
    "DATABASE",
    "DATABASES",
    "DATAFILE",
    "DATE",
    "DATETIME",
    "DAY",
    "DAY_HOUR",
    "DAY_MICROSECOND",
    "DAY_MINUTE",
    "DAY_SECOND",
    "DEALLOCATE",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFINER",
    "DELAYED",
    "DELAY_KEY_WRITE",
    "DELETE",
    "DELETE_DOMAIN_ID",
    "DESC",
    "DESCRIBE",
    "DES_KEY_FILE",
    "DETERMINISTIC",
    "DIAGNOSTICS",
    "DIRECTORY",
    "DISABLE",
    "DISCARD",
    "DISK",
    "DISTINCT",
    "DISTINCTROW",
    "DIV",
    "DO",
    "DOUBLE",
    "DO_DOMAIN_IDS",
    "DROP",
    "DUAL",
    "DUMPFILE",
    "DUPLICATE",
    "DYNAMIC",
    "EACH",
    "ELSE",
    "ELSEIF",
    "ELSIF",
    "EMPTY",
    "ENABLE",
    "ENCLOSED",
    "END",
    "ENDS",
    "ENGINE",
    "ENGINES",
    "ENUM",
    "ERROR",
    "ERRORS",
    "ESCAPE",
    "ESCAPED",
    "EVENT",
    "EVENTS",
    "EVERY",
    "EXAMINED",
    "EXCEPT",
    "EXCHANGE",
    "EXCLUDE",
    "EXECUTE",
    "EXCEPTION",
    "EXISTS",
    "EXIT",
    "EXPANSION",
    "EXPIRE",
    "EXPORT",
    "EXPLAIN",
    "EXTENDED",
    "EXTENT_SIZE",
    "FALSE",
    "FAST",
    "FAULTS",
    "FEDERATED",
    "FETCH",
    "FIELDS",
    "FILE",
    "FIRST",
    "FIXED",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "FLUSH",
    "FOLLOWING",
    "FOLLOWS",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FORMAT",
    "FOUND",
    "FROM",
    "FULL",
    "FULLTEXT",
    "FUNCTION",
    "GENERAL",
    "GENERATED",
    "GET_FORMAT",
    "GET",
    "GLOBAL",
    "GOTO",
    "GRANT",
    "GRANTS",
    "GROUP",
    "HANDLER",
    "HARD",
    "HASH",
    "HAVING",
    "HELP",
    "HIGH_PRIORITY",
    "HISTORY",
    "HOST",
    "HOSTS",
    "HOUR",
    "HOUR_MICROSECOND",
    "HOUR_MINUTE",
    "HOUR_SECOND",
    "ID",
    "IDENTIFIED",
    "IF",
    "IGNORE",
    "IGNORED",
    "IGNORE_DOMAIN_IDS",
    "IGNORE_SERVER_IDS",
    "IMMEDIATE",
    "IMPORT",
    "INTERSECT",
    "IN",
    "INCREMENT",
    "INDEX",
    "INDEXES",
    "INFILE",
    "INITIAL_SIZE",
    "INNER",
    "INOUT",
    "INSENSITIVE",
    "INSERT",
    "INSERT_METHOD",
    "INSTALL",
    "INT",
    "INT1",
    "INT2",
    "INT3",
    "INT4",
    "INT8",
    "INTEGER",
    "INTERVAL",
    "INVISIBLE",
    "INTO",
    "IO",
    "IO_THREAD",
    "IPC",
    "IS",
    "ISOLATION",
    "ISOPEN",
    "ISSUER",
    "ITERATE",
    "INVOKER",
    "JOIN",
    "JSON",
    "JSON_TABLE",
    "KEY",
    "KEYS",
    "KEY_BLOCK_SIZE",
    "KILL",
    "LANGUAGE",
    "LAST",
    "LAST_VALUE",
    "LASTVAL",
    "LEADING",
    "LEAVE",
    "LEAVES",
    "LEFT",
    "LESS",
    "LEVEL",
    "LIKE",
    "LIMIT",
    "LINEAR",
    "LINES",
    "LIST",
    "LOAD",
    "LOCAL",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCK",
    "LOCKED",
    "LOCKS",
    "LOGFILE",
    "LOGS",
    "LONG",
    "LONGBLOB",
    "LONGTEXT",
    "LOOP",
    "LOW_PRIORITY",
    "MASTER",
    "MASTER_CONNECT_RETRY",
    "MASTER_DELAY",
    "MASTER_GTID_POS",
    "MASTER_HOST",
    "MASTER_LOG_FILE",
    "MASTER_LOG_POS",
    "MASTER_PASSWORD",
    "MASTER_PORT",
    "MASTER_SERVER_ID",
    "MASTER_SSL",
    "MASTER_SSL_CA",
    "MASTER_SSL_CAPATH",
    "MASTER_SSL_CERT",
    "MASTER_SSL_CIPHER",
    "MASTER_SSL_CRL",
    "MASTER_SSL_CRLPATH",
    "MASTER_SSL_KEY",
    "MASTER_SSL_VERIFY_SERVER_CERT",
    "MASTER_USER",
    "MASTER_USE_GTID",
    "MASTER_HEARTBEAT_PERIOD",
    "MATCH",
    "MAX_CONNECTIONS_PER_HOUR",
    "MAX_QUERIES_PER_HOUR",
    "MAX_ROWS",
    "MAX_SIZE",
    "MAX_STATEMENT_TIME",
    "MAX_UPDATES_PER_HOUR",
    "MAX_USER_CONNECTIONS",
    "MAXVALUE",
    "MEDIUM",
    "MEDIUMBLOB",
    "MEDIUMINT",
    "MEDIUMTEXT",
    "MEMORY",
    "MERGE",
    "MESSAGE_TEXT",
    "MICROSECOND",
    "MIDDLEINT",
    "MIGRATE",
    "MINUS",
    "MINUTE",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "MINVALUE",
    "MIN_ROWS",
    "MOD",
    "MODE",
    "MODIFIES",
    "MODIFY",
    "MONITOR",
    "MONTH",
    "MUTEX",
    "MYSQL",
    "MYSQL_ERRNO",
    "NAME",
    "NAMES",
    "NATIONAL",
    "NATURAL",
    "NCHAR",
    "NESTED",
    "NEVER",
    "NEW",
    "NEXT",
    "NEXTVAL",
    "NO",
    "NOMAXVALUE",
    "NOMINVALUE",
    "NOCACHE",
    "NOCYCLE",
    "NO_WAIT",
    "NOWAIT",
    "NODEGROUP",
    "NONE",
    "NOT",
    "NOTFOUND",
    "NO_WRITE_TO_BINLOG",
    "NUMBER",
    "NUMERIC",
    "NVARCHAR",
    "OF",
    "OFFSET",
    "OLD_PASSWORD",
    "ON",
    "ONE",
    "ONLINE",
    "ONLY",
    "OPEN",
    "OPTIMIZE",
    "OPTIONS",
    "OPTION",
    "OPTIONALLY",
    "OR",
    "ORDER",
    "ORDINALITY",
    "OTHERS",
    "OUT",
    "OUTER",
    "OUTFILE",
    "OVER",
    "OVERLAPS",
    "OWNER",
    "PACKAGE",
    "PACK_KEYS",
    "PAGE",
    "PAGE_CHECKSUM",
    "PARSER",
    "PARSE_VCOL_EXPR",
    "PATH",
    "PERIOD",
    "PARTIAL",
    "PARTITION",
    "PARTITIONING",
    "PARTITIONS",
    "PASSWORD",
    "PERSISTENT",
    "PHASE",
    "PLUGIN",
    "PLUGINS",
    "PORT",
    "PORTION",
    "PRECEDES",
    "PRECEDING",
    "PRECISION",
    "PREPARE",
    "PRESERVE",
    "PREV",
    "PREVIOUS",
    "PRIMARY",
    "PRIVILEGES",
    "PROCEDURE",
    "PROCESS",
    "PROCESSLIST",
    "PROFILE",
    "PROFILES",
    "PROXY",
    "PURGE",
    "QUARTER",
    "QUERY",
    "QUICK",
    "RAISE",
    "RANGE",
    "RAW",
    "READ",
    "READ_ONLY",
    "READ_WRITE",
    "READS",
    "REAL",
    "REBUILD",
    "RECOVER",
    "RECURSIVE",
    "REDO_BUFFER_SIZE",
    "REDOFILE",
    "REDUNDANT",
    "REFERENCES",
    "REGEXP",
    "RELAY",
    "RELAYLOG",
    "RELAY_LOG_FILE",
    "RELAY_LOG_POS",
    "RELAY_THREAD",
    "RELEASE",
    "RELOAD",
    "REMOVE",
    "RENAME",
    "REORGANIZE",
    "REPAIR",
    "REPEATABLE",
    "REPLACE",
    "REPLAY",
    "REPLICA",
    "REPLICAS",
    "REPLICA_POS",
    "REPLICATION",
    "REPEAT",
    "REQUIRE",
    "RESET",
    "RESIGNAL",
    "RESTART",
    "RESTORE",
    "RESTRICT",
    "RESUME",
    "RETURNED_SQLSTATE",
    "RETURN",
    "RETURNING",
    "RETURNS",
    "REUSE",
    "REVERSE",
    "REVOKE",
    "RIGHT",
    "RLIKE",
    "ROLE",
    "ROLLBACK",
    "ROLLUP",
    "ROUTINE",
    "ROW",
    "ROWCOUNT",
    "ROWNUM",
    "ROWS",
    "ROWTYPE",
    "ROW_COUNT",
    "ROW_FORMAT",
    "RTREE",
    "SAVEPOINT",
    "SCHEDULE",
    "SCHEMA",
    "SCHEMA_NAME",
    "SCHEMAS",
    "SECOND",
    "SECOND_MICROSECOND",
    "SECURITY",
    "SELECT",
    "SENSITIVE",
    "SEPARATOR",
    "SEQUENCE",
    "SERIAL",
    "SERIALIZABLE",
    "SESSION",
    "SERVER",
    "SET",
    "SETVAL",
    "SHARE",
    "SHOW",
    "SHUTDOWN",
    "SIGNAL",
    "SIGNED",
    "SIMPLE",
    "SKIP",
    "SLAVE",
    "SLAVES",
    "SLAVE_POS",
    "SLOW",
    "SNAPSHOT",
    "SMALLINT",
    "SOCKET",
    "SOFT",
    "SOME",
    "SONAME",
    "SOUNDS",
    "SOURCE",
    "STAGE",
    "STORED",
    "SPATIAL",
    "SPECIFIC",
    "REF_SYSTEM_ID",
    "SQL",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "SQL_BIG_RESULT",
    "SQL_BUFFER_RESULT",
    "SQL_CACHE",
    "SQL_CALC_FOUND_ROWS",
    "SQL_NO_CACHE",
    "SQL_SMALL_RESULT",
    "SQL_THREAD",
    "SQL_TSI_SECOND",
    "SQL_TSI_MINUTE",
    "SQL_TSI_HOUR",
    "SQL_TSI_DAY",
    "SQL_TSI_WEEK",
    "SQL_TSI_MONTH",
    "SQL_TSI_QUARTER",
    "SQL_TSI_YEAR",
    "SSL",
    "START",
    "STARTING",
    "STARTS",
    "STATEMENT",
    "STATS_AUTO_RECALC",
    "STATS_PERSISTENT",
    "STATS_SAMPLE_PAGES",
    "STATUS",
    "STOP",
    "STORAGE",
    "STRAIGHT_JOIN",
    "STRING",
    "SUBCLASS_ORIGIN",
    "SUBJECT",
    "SUBPARTITION",
    "SUBPARTITIONS",
    "SUPER",
    "SUSPEND",
    "SWAPS",
    "SWITCHES",
    "SYSDATE",
    "SYSTEM",
    "SYSTEM_TIME",
    "TABLE",
    "TABLE_NAME",
    "TABLES",
    "TABLESPACE",
    "TABLE_CHECKSUM",
    "TEMPORARY",
    "TEMPTABLE",
    "TERMINATED",
    "TEXT",
    "THAN",
    "THEN",
    "TIES",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMPADD",
    "TIMESTAMPDIFF",
    "TINYBLOB",
    "TINYINT",
    "TINYTEXT",
    "TO",
    "TRAILING",
    "TRANSACTION",
    "TRANSACTIONAL",
    "THREADS",
    "TRIGGER",
    "TRIGGERS",
    "TRUE",
    "TRUNCATE",
    "TYPE",
    "TYPES",
    "UNBOUNDED",
    "UNCOMMITTED",
    "UNDEFINED",
    "UNDO_BUFFER_SIZE",
    "UNDOFILE",
    "UNDO",
    "UNICODE",
    "UNION",
    "UNIQUE",
    "UNKNOWN",
    "UNLOCK",
    "UNINSTALL",
    "UNSIGNED",
    "UNTIL",
    "UPDATE",
    "UPGRADE",
    "USAGE",
    "USE",
    "USER",
    "USER_RESOURCES",
    "USE_FRM",
    "USING",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "VALUE",
    "VALUES",
    "VARBINARY",
    "VARCHAR",
    "VARCHARACTER",
    "VARCHAR2",
    "VARIABLES",
    "VARYING",
    "VIA",
    "VIEW",
    "VIRTUAL",
    "VISIBLE",
    "VERSIONING",
    "WAIT",
    "WARNINGS",
    "WEEK",
    "WEIGHT_STRING",
    "WHEN",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WITHIN",
    "WITHOUT",
    "WORK",
    "WRAPPER",
    "WRITE",
    "X509",
    "XOR",
    "XA",
    "XML",
    "YEAR",
    "YEAR_MONTH",
    "ZEROFILL",
    "||",
]
default_functions = [
    "ADDDATE",
    "ADD_MONTHS",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "CAST",
    "COUNT",
    "CUME_DIST",
    "CURDATE",
    "CURTIME",
    "DATE_ADD",
    "DATE_SUB",
    "DATE_FORMAT",
    "DECODE",
    "DENSE_RANK",
    "EXTRACT",
    "FIRST_VALUE",
    "GROUP_CONCAT",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "LAG",
    "LEAD",
    "MAX",
    "MEDIAN",
    "MID",
    "MIN",
    "NOW",
    "NTH_VALUE",
    "NTILE",
    "POSITION",
    "PERCENT_RANK",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "RANK",
    "ROW_NUMBER",
    "SESSION_USER",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "SUBDATE",
    "SUBSTR",
    "SUBSTRING",
    "SUM",
    "SYSTEM_USER",
    "TRIM",
    "TRIM_ORACLE",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "ABS",
    "ACOS",
    "ADDTIME",
    "AES_DECRYPT",
    "AES_ENCRYPT",
    "ASIN",
    "ATAN",
    "ATAN2",
    "BENCHMARK",
    "BIN",
    "BINLOG_GTID_POS",
    "BIT_COUNT",
    "BIT_LENGTH",
    "CEIL",
    "CEILING",
    "CHARACTER_LENGTH",
    "CHAR_LENGTH",
    "CHR",
    "COERCIBILITY",
    "COLUMN_CHECK",
    "COLUMN_EXISTS",
    "COLUMN_LIST",
    "COLUMN_JSON",
    "COMPRESS",
    "CONCAT",
    "CONCAT_OPERATOR_ORACLE",
    "CONCAT_WS",
    "CONNECTION_ID",
    "CONV",
    "CONVERT_TZ",
    "COS",
    "COT",
    "CRC32",
    "DATEDIFF",
    "DAYNAME",
    "DAYOFMONTH",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "DEGREES",
    "DECODE_HISTOGRAM",
    "DECODE_ORACLE",
    "DES_DECRYPT",
    "DES_ENCRYPT",
    "ELT",
    "ENCODE",
    "ENCRYPT",
    "EXP",
    "EXPORT_SET",
    "EXTRACTVALUE",
    "FIELD",
    "FIND_IN_SET",
    "FLOOR",
    "FORMAT",
    "FOUND_ROWS",
    "FROM_BASE64",
    "FROM_DAYS",
    "FROM_UNIXTIME",
    "GET_LOCK",
    "GREATEST",
    "HEX",
    "IFNULL",
    "INSTR",
    "ISNULL",
    "IS_FREE_LOCK",
    "IS_USED_LOCK",
    "JSON_ARRAY",
    "JSON_ARRAY_APPEND",
    "JSON_ARRAY_INSERT",
    "JSON_COMPACT",
    "JSON_CONTAINS",
    "JSON_CONTAINS_PATH",
    "JSON_DEPTH",
    "JSON_DETAILED",
    "JSON_EXISTS",
    "JSON_EXTRACT",
    "JSON_INSERT",
    "JSON_KEYS",
    "JSON_LENGTH",
    "JSON_LOOSE",
    "JSON_MERGE",
    "JSON_MERGE_PATCH",
    "JSON_MERGE_PRESERVE",
    "JSON_QUERY",
    "JSON_QUOTE",
    "JSON_OBJECT",
    "JSON_REMOVE",
    "JSON_REPLACE",
    "JSON_SET",
    "JSON_SEARCH",
    "JSON_TYPE",
    "JSON_UNQUOTE",
    "JSON_VALID",
    "JSON_VALUE",
    "LAST_DAY",
    "LAST_INSERT_ID",
    "LCASE",
    "LEAST",
    "LENGTH",
    "LENGTHB",
    "LN",
    "LOAD_FILE",
    "LOCATE",
    "LOG",
    "LOG10",
    "LOG2",
    "LOWER",
    "LPAD",
    "LPAD_ORACLE",
    "LTRIM",
    "LTRIM_ORACLE",
    "MAKEDATE",
    "MAKETIME",
    "MAKE_SET",
    "MASTER_GTID_WAIT",
    "MASTER_POS_WAIT",
    "MD5",
    "MONTHNAME",
    "NAME_CONST",
    "NVL",
    "NVL2",
    "NULLIF",
    "OCT",
    "OCTET_LENGTH",
    "ORD",
    "PERIOD_ADD",
    "PERIOD_DIFF",
    "PI",
    "POW",
    "POWER",
    "QUOTE",
    "REGEXP_INSTR",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "RADIANS",
    "RAND",
    "RELEASE_ALL_LOCKS",
    "RELEASE_LOCK",
    "REPLACE_ORACLE",
    "REVERSE",
    "ROUND",
    "RPAD",
    "RPAD_ORACLE",
    "RTRIM",
    "RTRIM_ORACLE",
    "SEC_TO_TIME",
    "SHA",
    "SHA1",
    "SHA2",
    "SIGN",
    "SIN",
    "SLEEP",
    "SOUNDEX",
    "SPACE",
    "SQRT",
    "STRCMP",
    "STR_TO_DATE",
    "SUBSTR_ORACLE",
    "SUBSTRING_INDEX",
    "SUBTIME",
    "SYS_GUID",
    "TAN",
    "TIMEDIFF",
    "TIME_FORMAT",
    "TIME_TO_SEC",
    "TO_BASE64",
    "TO_CHAR",
    "TO_DAYS",
    "TO_SECONDS",
    "UCASE",
    "UNCOMPRESS",
    "UNCOMPRESSED_LENGTH",
    "UNHEX",
    "UNIX_TIMESTAMP",
    "UPDATEXML",
    "UPPER",
    "UUID",
    "UUID_SHORT",
    "VERSION",
    "WEEKDAY",
    "WEEKOFYEAR",
    "WSREP_LAST_WRITTEN_GTID",
    "WSREP_LAST_SEEN_GTID",
    "WSREP_SYNC_WAIT_UPTO_GTID",
    "YEARWEEK",
]


class SQLAnalyze(Completer):
    keywords = []

    functions = []

    show_items = []

    change_items = []

    users = []

    def __init__(
        self, log, smart_completion=True, supported_formats=(), keyword_casing="auto"
    ):
        self.smart_completion = smart_completion
        self.reserved_words = set()
        for x in self.keywords:
            self.reserved_words.update(x.split())
        self.name_pattern = compile(r"^[_a-z][_a-z0-9\$]*$")

        self.special_commands = []
        self.table_formats = supported_formats
        if keyword_casing not in ("upper", "lower", "auto"):
            keyword_casing = "auto"
        self.keyword_casing = keyword_casing
        self.reset_completions()
        self.set_keywords(default_keywords)
        self.set_functions(default_functions)
        self.log = log

    # can't set empty list
    def set_keywords(self, keywords: List[str]):
        if len(keywords) > 0:
            # part of keywords is belong to change_items
            # part of change_items is prefix with master_
            change_items = [
                "RELAY_LOG_FILE",
                "RELAY_LOG_POS",
                "IGNORE_SERVER_IDS",
                "IGNORE_DOMAIN_IDS",
                "DO_DOMAIN_IDS",
            ]
            temp_keywords = keywords
            master_prefix_items = list(
                filter(lambda item: item.startswith("MASTER_"), temp_keywords)
            )
            temp_keywords = list(
                filter(
                    lambda item: item not in change_items
                    and not item.startswith("MASTER_"),
                    temp_keywords,
                )
            )
            self.keywords = temp_keywords
            self.change_items = change_items + master_prefix_items

    # can't set empty list
    def set_functions(self, functions: List[str]):
        if len(functions) > 0:
            self.functions = functions

    def escape_name(self, name):
        if name and (
            (not self.name_pattern.match(name))
            or (name.upper() in self.reserved_words)
            or (name.upper() in self.functions)
        ):
            name = "`%s`" % name

        return name

    def unescape_name(self, name):
        """Unquote a string."""
        if name and name[0] == '"' and name[-1] == '"':
            name = name[1:-1]

        return name

    def escaped_names(self, names):
        return [self.escape_name(name) for name in names]

    def extend_special_commands(self, special_commands):
        # Special commands are not part of all_completions since they can only
        # be at the beginning of a line.
        self.special_commands.extend(special_commands)

    def extend_database_names(self, databases):
        self.databases.extend(databases)

    def extend_keywords(self, additional_keywords):
        self.keywords.extend(additional_keywords)
        self.all_completions.update(additional_keywords)

    def extend_show_items(self, show_items):
        for show_item in show_items:
            self.show_items.extend(show_item)
            self.all_completions.update(show_item)

    def extend_change_items(self, change_items):
        for change_item in change_items:
            self.change_items.extend(change_item)
            self.all_completions.update(change_item)

    def extend_users(self, users):
        for user in users:
            self.users.extend(user)
            self.all_completions.update(user)

    def extend_schemata(self, schema):
        if schema is None:
            return
        metadata = self.dbmetadata["tables"]
        metadata[schema] = {}

        # dbmetadata.values() are the 'tables' and 'functions' dicts
        for metadata in self.dbmetadata.values():
            metadata[schema] = {}
        self.all_completions.update(schema)

    def extend_relations(self, data, kind):
        """Extend metadata for tables or views

        :param data: list of (rel_name, ) tuples
        :param kind: either 'tables' or 'views'
        :return:
        """
        # 'data' is a generator object. It can throw an exception while being
        # consumed. This could happen if the user has launched the app without
        # specifying a database name. This exception must be handled to prevent
        # crashing.
        try:
            data = [self.escaped_names(d) for d in data]
        except Exception:
            data = []

        # dbmetadata['tables'][$schema_name][$table_name] should be a list of
        # column names. Default to an asterisk
        metadata = self.dbmetadata[kind]
        for relname in data:
            try:
                metadata[self.dbname][relname[0]] = ["*"]
            except KeyError:
                print(
                    "%r %r listed in unrecognized schema %r",
                    kind,
                    relname[0],
                    self.dbname,
                )
            self.all_completions.add(relname[0])

    def extend_columns(self, column_data, kind):
        """Extend column metadata

        :param column_data: list of (rel_name, column_name) tuples
        :param kind: either 'tables' or 'views'
        :return:
        """
        # 'column_data' is a generator object. It can throw an exception while
        # being consumed. This could happen if the user has launched the app
        # without specifying a database name. This exception must be handled to
        # prevent crashing.
        try:
            column_data = [self.escaped_names(d) for d in column_data]
        except Exception:
            column_data = []

        metadata = self.dbmetadata[kind]
        for relname, column in column_data:
            metadata[self.dbname][relname].append(column)
            self.all_completions.add(column)

    def extend_functions(self, func_data):
        # 'func_data' is a generator object. It can throw an exception while
        # being consumed. This could happen if the user has launched the app
        # without specifying a database name. This exception must be handled to
        # prevent crashing.
        try:
            func_data = [self.escaped_names(d) for d in func_data]
        except Exception:
            func_data = []

        # dbmetadata['functions'][$schema_name][$function_name] should return
        # function metadata.
        metadata = self.dbmetadata["functions"]

        for func in func_data:
            metadata[self.dbname][func[0]] = None
            self.all_completions.add(func[0])

    def extend_tables(self, table_data):
        try:
            table_data = [self.escaped_names(d) for d in table_data]
        except Exception:
            table_data = []
        for database, table in table_data:
            self.database_tables.append((database, table))

    def extend_global_variables(self, variable_data):
        self.global_variable.extend(variable_data)

    def extend_session_variables(self, variable_data):
        self.session_variable.extend(variable_data)

    def set_dbname(self, dbname):
        self.dbname = dbname

    def reset_completions(self, completer=None):
        if completer:
            self.databases = completer.databases
            self.database_tables: List[Tuple[str, str]] = completer.database_tables
            self.global_variable: List[str] = completer.global_variable
            self.session_variable: List[str] = completer.session_variable
            self.users = completer.users
            self.show_items = completer.show_items
            self.dbname = completer.dbname
            self.dbmetadata = completer.dbmetadata
            self.all_completions = completer.all_completions
        else:
            self.databases = []
            self.database_tables: List[Tuple[str, str]] = []
            self.global_variable: List[str] = []
            self.session_variable: List[str] = []
            self.users = []
            self.show_items = []
            self.dbname = ""
            self.dbmetadata = {"tables": {}, "views": {}, "functions": {}}
            self.all_completions = set(self.keywords + self.functions)

    @staticmethod
    def find_matches(text, collection, start_only=False, fuzzy=True, casing=None):
        """Find completion matches for the given text.

        Given the user's input text and a collection of available
        completions, find completions matching the last word of the
        text.

        If `start_only` is True, the text will match an available
        completion only at the beginning. Otherwise, a completion is
        considered a match if the text appears anywhere within it.

        yields prompt_toolkit Completion instances for any matches found
        in the collection of available completions.
        """
        last = last_word(text, include="most_punctuations")
        text = last.lower()

        completions = []

        if fuzzy:
            regex = ".*?".join(map(escape, text))
            pat = compile("(%s)" % regex)
            for item in sorted(collection):
                r = pat.search(item.lower())
                if r:
                    completions.append((len(r.group()), r.start(), item))
        else:
            match_end_limit = len(text) if start_only else None
            for item in sorted(collection):
                match_point = item.lower().find(text, 0, match_end_limit)
                if match_point >= 0:
                    completions.append((len(text), match_point, item))

        if casing == "auto":
            casing = "lower" if last and last[-1].islower() else "upper"

        def apply_case(kw):
            if casing == "upper":
                return kw.upper()
            return kw.lower()

        return (
            Completion(z if casing is None else apply_case(z), -len(text))
            for x, y, z in sorted(completions)
        )

    def extend_with_type(
        self, list: List, completions: Generator[Completion, None, None], type: str
    ):
        new_comletion_list = []
        for completion in completions:
            new_comletion_list.append(
                Completion(
                    text=completion.text,
                    start_position=completion.start_position,
                    display_meta=type,
                )
            )
        list.extend(new_comletion_list)

    def get_completions(
        self, document, complete_event, executor: SqlFetch, smart_completion=None
    ):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        if smart_completion is None:
            smart_completion = self.smart_completion

        # If smart_completion is off then match any word that starts with
        # 'word_before_cursor'.
        if not smart_completion:
            return self.find_matches(
                word_before_cursor, self.all_completions, start_only=True, fuzzy=False
            )

        completions = []
        suggestions = suggest_type(document.text, document.text_before_cursor)

        for suggestion in suggestions:

            if suggestion["type"] == "column":
                tables = suggestion["tables"]
                scoped_cols = self.populate_scoped_cols(tables)
                if suggestion.get("drop_unique"):
                    # drop_unique is used for 'tb11 JOIN tbl2 USING (...'
                    # which should suggest only columns that appear in more than
                    # one table
                    scoped_cols = [
                        col
                        for (col, count) in Counter(scoped_cols).items()
                        if count > 1 and col != "*"
                    ]
                # actively fetch system table's column
                if len(tables) > 0:
                    database = tables[0][0]
                    table = tables[0][1]
                    if database and table:
                        database_table_dict = self.dbmetadata["tables"].get(database)
                        if database_table_dict is None:
                            # fetch the table's column
                            column_list = executor.get_specific_table_columns_list(
                                table, database
                            )
                            scoped_cols.extend(column_list)
                cols = self.find_matches(word_before_cursor, scoped_cols)
                self.extend_with_type(completions, cols, suggestion["type"])

            elif suggestion["type"] == "function":
                # suggest user-defined functions using substring matching
                funcs = self.populate_schema_objects(suggestion["schema"], "functions")
                user_funcs = self.find_matches(word_before_cursor, funcs)
                self.extend_with_type(completions, user_funcs, suggestion["type"])

                # suggest hardcoded functions using startswith matching only if
                # there is no schema qualifier. If a schema qualifier is
                # present it probably denotes a table.
                # eg: SELECT * FROM users u WHERE u.
                if not suggestion["schema"]:
                    predefined_funcs = self.find_matches(
                        word_before_cursor,
                        self.functions,
                        start_only=True,
                        fuzzy=False,
                        casing=self.keyword_casing,
                    )
                    self.extend_with_type(
                        completions, predefined_funcs, suggestion["type"]
                    )

            elif suggestion["type"] == "table":
                tables = self.populate_schema_objects(suggestion["schema"], "tables")
                if len(tables) == 0:
                    # find in database_tables
                    if len(suggestion["schema"]) > 0:
                        db_name = suggestion["schema"].rstrip(".")
                        tables.extend(
                            [t[1] for t in self.database_tables if t[0] == db_name]
                        )
                tables = self.find_matches(word_before_cursor, tables)
                self.extend_with_type(completions, tables, suggestion["type"])

            elif suggestion["type"] == "view":
                views = self.populate_schema_objects(suggestion["schema"], "views")
                views = self.find_matches(word_before_cursor, views)
                self.extend_with_type(completions, views, suggestion["type"])

            elif suggestion["type"] == "alias":
                aliases = suggestion["aliases"]
                aliases = self.find_matches(word_before_cursor, aliases)
                self.extend_with_type(completions, aliases, suggestion["type"])

            elif suggestion["type"] == "database":
                if suggestion.get("table") != None:
                    if suggestion.get("table") == "":
                        dbs = self.find_matches(word_before_cursor, self.databases)
                        self.extend_with_type(completions, dbs, suggestion["type"])
                    else:
                        database_list = list(
                            map(
                                lambda t: t[0],
                                filter(
                                    lambda t: t[1] == suggestion["table"],
                                    self.database_tables,
                                ),
                            )
                        )
                        dbs = self.find_matches(word_before_cursor, database_list)
                        self.extend_with_type(completions, dbs, suggestion["type"])
                else:
                    dbs = self.find_matches(word_before_cursor, self.databases)
                    self.extend_with_type(completions, dbs, suggestion["type"])

            elif suggestion["type"] == "keyword":
                keywords = self.find_matches(
                    word_before_cursor,
                    self.keywords,
                    start_only=True,
                    fuzzy=False,
                    casing=self.keyword_casing,
                )
                self.extend_with_type(completions, keywords, suggestion["type"])

            elif suggestion["type"] == "show":
                show_items = self.find_matches(
                    word_before_cursor,
                    self.show_items,
                    start_only=False,
                    fuzzy=True,
                    casing=self.keyword_casing,
                )
                self.extend_with_type(completions, show_items, suggestion["type"])

            elif suggestion["type"] == "change":
                change_items = self.find_matches(
                    word_before_cursor, self.change_items, start_only=False, fuzzy=True
                )
                self.extend_with_type(completions, change_items, suggestion["type"])
            elif suggestion["type"] == "user":
                users = self.find_matches(
                    word_before_cursor, self.users, start_only=False, fuzzy=True
                )
                self.extend_with_type(completions, users, suggestion["type"])

            elif suggestion["type"] == "special":
                special = self.find_matches(
                    word_before_cursor,
                    self.special_commands,
                    start_only=True,
                    fuzzy=False,
                )
                self.extend_with_type(completions, special, suggestion["type"])
            elif suggestion["type"] == "favoritequery":
                queries = self.find_matches(
                    word_before_cursor,
                    FavoriteQueries.instance.list(),
                    start_only=False,
                    fuzzy=True,
                )
                self.extend_with_type(completions, queries, suggestion["type"])
            elif suggestion["type"] == "table_format":
                formats = self.find_matches(
                    word_before_cursor, self.table_formats, start_only=True, fuzzy=False
                )
                self.extend_with_type(completions, formats, suggestion["type"])
            elif suggestion["type"] == "file_name":
                file_names = self.find_files(word_before_cursor)
                self.extend_with_type(completions, file_names, suggestion["type"])
            elif suggestion["type"] == "session":
                # [{'scope': 'both', 'type': 'session'}]
                # scope can be both, global, session
                # use set because global_variable would overlap with session_variable
                variable_list = set()
                if suggestion["scope"] == "global":
                    variable_list = {*variable_list, *self.global_variable}
                elif suggestion["scope"] == "session":
                    variable_list = {*variable_list, *self.session_variable}
                else:
                    variable_list = {
                        *variable_list,
                        *self.global_variable,
                        *self.session_variable,
                    }
                # word_before_cursor would be @@
                word = (
                    word_before_cursor.replace("@@global.", "")
                    .replace("@@session.", "")
                    .replace("@@", "")
                )
                variables = self.find_matches(word, list(variable_list))
                self.extend_with_type(completions, variables, suggestion["type"])

        return completions

    def find_files(self, word):
        """Yield matching directory or file names.

        :param word:
        :return: iterable

        """
        base_path, last_path, position = parse_path(word)
        paths = suggest_path(word)
        for name in sorted(paths):
            suggestion = complete_path(name, last_path)
            if suggestion:
                yield Completion(suggestion, position)

    def populate_scoped_cols(self, scoped_tbls):
        """Find all columns in a set of scoped_tables
        :param scoped_tbls: list of (schema, table, alias) tuples
        :return: list of column names
        """
        columns = []
        meta = self.dbmetadata

        for tbl in scoped_tbls:
            # A fully qualified schema.relname reference or default_schema
            # DO NOT escape schema names.
            schema = tbl[0] or self.dbname
            relname = tbl[1]
            escaped_relname = self.escape_name(tbl[1])

            # We don't know if schema.relname is a table or view. Since
            # tables and views cannot share the same name, we can check one
            # at a time
            try:
                columns.extend(meta["tables"][schema][relname])

                # Table exists, so don't bother checking for a view
                continue
            except KeyError:
                try:
                    columns.extend(meta["tables"][schema][escaped_relname])
                    # Table exists, so don't bother checking for a view
                    continue
                except KeyError:
                    pass

            try:
                columns.extend(meta["views"][schema][relname])
            except KeyError:
                pass

        return columns

    def populate_schema_objects(self, schema, obj_type):
        """Returns list of tables or functions for a (optional) schema"""
        metadata = self.dbmetadata[obj_type]
        schema = schema or self.dbname

        try:
            objects = metadata[schema].keys()
        except KeyError:
            # schema doesn't exist
            objects = []

        return objects
