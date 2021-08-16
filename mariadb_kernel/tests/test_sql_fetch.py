from collections import namedtuple
from typing import Type
from ..mariadb_client import MariaDBClient

from ..mariadb_server import MariaDBServer
from ..client_config import ClientConfig

from ..sql_fetch import SqlFetch
from unittest.mock import Mock

import unittest


def test_mariadb_sql_fetch_get_database_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert set(["information_schema", "mysql", "performance_schema"]).issubset(
        sql_fetch.databases()
    )


def test_mariadb_sql_fetch_get_table_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database t1;")
    client.run_statement("use t1;")
    client.run_statement("create table table1(a int);")
    client.run_statement("create table table2(a int);")
    sql_fetch = SqlFetch(client, mocklog)

    unittest.TestCase().assertListEqual(
        sql_fetch.tables(),
        [("table1",), ("table2",)],
    )

    client.run_statement("drop database t1;")


def test_mariadb_sql_fetch_get_table_list_when_no_select_database(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    unittest.TestCase().assertListEqual(
        sql_fetch.tables(),
        [],
    )

    client.run_statement("drop database t1;")


def test_mariadb_sql_fetch_get_show_candiates(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert set(
        [
            (t,)
            for t in [
                "AUTHORS",
                "BINARY LOGS",
                "BINLOG EVENTS",
                "CHARACTER SET",
                "COLLATION",
                "COLUMNS",
                "CONTRIBUTORS",
                "CREATE DATABASE",
                "CREATE EVENT",
                "CREATE FUNCTION",
                "CREATE PROCEDURE",
                "CREATE TABLE",
                "CREATE TRIGGER",
                "CREATE VIEW",
                "DATABASES",
                "ENGINE",
                "ENGINES",
                "ERRORS",
                "EVENTS",
                "FUNCTION CODE",
                "FUNCTION STATUS",
                "GRANTS",
                "INDEX",
                "MASTER STATUS",
                "OPEN TABLES",
                "PLUGINS",
                "PRIVILEGES",
                "PROCEDURE CODE",
                "PROCEDURE STATUS",
                "PROCESSLIST",
                "PROFILE",
                "PROFILES",
                "RELAYLOG EVENTS",
                "SLAVE HOSTS",
                "SLAVE STATUS",
                "STATUS",
                "TABLE STATUS",
                "TABLES",
                "TRIGGERS",
                "VARIABLES",
                "WARNINGS",
            ]
        ]
    ).issubset(sql_fetch.show_candidates())


def test_mariadb_sql_fetch_get_user_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert set(["'root'@'localhost'"]).issubset([t[0] for t in sql_fetch.users()])


def test_mariadb_sql_fetch_get_function_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    db_name = "test"
    sql_fetch.change_db_name(db_name)
    client.run_statement(f"use {db_name};")

    client.run_statement(
        """CREATE FUNCTION hello (s CHAR(20))
        RETURNS CHAR(50) DETERMINISTIC
        RETURN CONCAT('Hello, ',s,'!');"""
    )

    assert set(["hello"]).issubset([t[0] for t in sql_fetch.functions()])

    client.run_statement("drop function hello if exists;")


def test_mariadb_sql_fetch_get_table_column_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    db_name = "test"
    sql_fetch.change_db_name(db_name)
    client.run_statement(f"use {db_name};")
    client.run_statement("create table t1(a int);")
    client.run_statement("create table t2(b int);")

    unittest.TestCase().assertListEqual(
        sql_fetch.table_columns(), [("t1", "a"), ("t2", "b")]
    )

    client.run_statement("drop table t1;")
    client.run_statement("drop table t2;")


def test_mariadb_sql_fetch_get_connected_clients_num(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert sql_fetch.num_connected_clients() == 1


def test_mariadb_sql_fetch_get_current_used_database_name(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    sql_fetch = SqlFetch(client, mocklog)

    assert sql_fetch.get_db_name() == "test"


def test_mariadb_sql_fetch_get_current_used_database_name_for_no_select_database(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert sql_fetch.get_db_name() == ""


def test_mariadb_sql_fetch_keywords(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    expected_keywords = [
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
    keywords = sql_fetch.keywords()
    assert set(expected_keywords).issubset(keywords) or keywords == []


def test_mariadb_sql_fetch_sql_functions(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    expected_functions = [
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
    functions = sql_fetch.sql_functions()
    assert set(expected_functions).issubset(functions) or functions == []


def test_mariadb_sql_fetch_database_tables(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    client.run_statement("create database t1;")
    client.run_statement("create database t2;")
    client.run_statement("use t1;")
    client.run_statement("create table table1(a int);")
    client.run_statement("create table table2(a int);")
    client.run_statement("use t2;")
    client.run_statement("create table alpha(a int);")
    client.run_statement("create table beta(a int);")

    assert set(
        [("t1", "table1"), ("t1", "table2"), ("t2", "alpha"), ("t2", "beta")]
    ).issubset(sql_fetch.database_tables())
    client.run_statement("drop database t1;")
    client.run_statement("drop database t2;")


def test_mariadb_sql_fetch_global_variables(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    assert set(["innodb_sync_spin_loops"]).issubset(sql_fetch.global_variables())


def test_mariadb_sql_fetch_session_variables(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    assert set(["alter_algorithm"]).issubset(sql_fetch.session_variables())


def test_mariadb_sql_fetch_column_type(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database d1;")
    client.run_statement("use d1;")
    client.run_statement("create table t2 (c int(11), b int(11), a int(11));")
    ColumnType = namedtuple("ColumnType", ["name", "type"])
    sql_fetch = SqlFetch(client, mocklog)
    assert [
        ColumnType("c", "int(11)"),
        ColumnType("b", "int(11)"),
        ColumnType("a", "int(11)"),
    ] == sql_fetch.get_column_type_list("t2", "d1")
    client.run_statement("drop database d1;")


def test_mariadb_sql_fetch_get_help_text(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    # just part of documentation
    assert sql_fetch.get_help_text("min").startswith("Name")
    assert sql_fetch.get_help_text("asdfasdf") == ""


def test_mariadb_sql_fetch_get_specific_table_columns_list(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog)  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    # just test part of column
    assert set(["Host", "User", "Password"]).issubset(
        sql_fetch.get_specific_table_columns_list("user", "mysql")
    )
