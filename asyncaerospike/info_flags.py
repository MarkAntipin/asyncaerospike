from enum import IntFlag


class Info1Flags(IntFlag):
    EMPTY = 0
    READ = 1
    GET_ALL = 2
    BATCH_INDEX = 8
    XDR = 16
    DONT_GET_BIN_DATA = 32
    READ_MODE_AP_ALL = 64
    COMPRESS_RESPONSE = 128


class Info2Flags(IntFlag):
    EMPTY = 0
    WRITE = 1
    DELETE = 2
    GENERATION = 4
    GENERATION_GT = 8
    DURABLE_DELETE = 16
    CREATE_ONLY = 32
    RESPOND_ALL_OPS = 128


class Info3Flags(IntFlag):
    EMPTY = 0
    LAST = 1
    COMMIT_MASTER = 2
    PARTITION_DON = 4
    UPDATE_ONLY = 8
    CREATE_OR_REPLACE = 16
    REPLACE_ONLY = 32
    SC_READ_TYPE = 64
    SC_READ_RELAX = 128

