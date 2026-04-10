"""
conftest.py — pytest configuration for local unit tests.

pyflink and confluent_kafka are only available inside Docker containers.
This file registers lightweight stub modules/classes for those packages
BEFORE any test module is imported, so pytest can collect and run all unit
tests in a local Python environment without a running Flink or Kafka cluster.

Using real stub classes (not MagicMocks) is critical: LiquidityPositionFunction
inherits from KeyedBroadcastProcessFunction and stores instance state via
self.balance_state = ... . If the base class is a MagicMock, __setattr__
gets intercepted and assignments are lost.
"""

import sys
from types import ModuleType
from unittest.mock import MagicMock


# ─────────────────────────────────────────────────────────────────
#  Helper to create a real (empty) module and register it
# ─────────────────────────────────────────────────────────────────
def _make_module(name):
    mod = ModuleType(name)
    sys.modules[name] = mod
    return mod


# ─────────────────────────────────────────────────────────────────
#  pyflink.datastream.functions — base classes for Flink operators
#  These MUST be real Python classes (not MagicMocks) so that
#  LiquidityPositionFunction can inherit from them correctly.
# ─────────────────────────────────────────────────────────────────
class KeyedBroadcastProcessFunction:
    class Context: pass
    class ReadOnlyContext: pass
    class OnTimerContext: pass


class KeyedProcessFunction:
    class Context: pass
    class OnTimerContext: pass


class BroadcastProcessFunction:
    class Context: pass


class RuntimeContext: pass
class AggregateFunction: pass
class ProcessWindowFunction: pass


# ─────────────────────────────────────────────────────────────────
#  pyflink.datastream.state — descriptor classes
#  Must be real classes so .name works correctly in MockRuntimeContext
# ─────────────────────────────────────────────────────────────────
class ValueStateDescriptor:
    def __init__(self, name, type_info):
        self.name = name


class MapStateDescriptor:
    def __init__(self, name, key_type, value_type=None):
        self.name = name


# ─────────────────────────────────────────────────────────────────
#  pyflink.common.typeinfo — Types constants
# ─────────────────────────────────────────────────────────────────
class Types:
    @staticmethod
    def STRING(): return "STRING"
    @staticmethod
    def DOUBLE(): return "DOUBLE"
    @staticmethod
    def INT():    return "INT"
    @staticmethod
    def LONG():   return "LONG"
    @staticmethod
    def MAP(*args): return "MAP"
    @staticmethod
    def BOOLEAN(): return "BOOLEAN"


# ─────────────────────────────────────────────────────────────────
#  Stub everything else as simple pass-through classes
# ─────────────────────────────────────────────────────────────────
class _Stub:
    """Generic stub — accepts any args, does nothing."""
    def __init__(self, *a, **kw): pass
    @classmethod
    def builder(cls): return cls()
    def __getattr__(self, name):
        return lambda *a, **kw: self


# ─────────────────────────────────────────────────────────────────
#  Register all pyflink sub-modules
# ─────────────────────────────────────────────────────────────────
_pyflink              = _make_module("pyflink")
_pyflink_common       = _make_module("pyflink.common")
_pyflink_common_ws    = _make_module("pyflink.common.watermark_strategy")
_pyflink_common_ser   = _make_module("pyflink.common.serialization")
_pyflink_common_ti    = _make_module("pyflink.common.typeinfo")
_pyflink_ds           = _make_module("pyflink.datastream")
_pyflink_ds_conn      = _make_module("pyflink.datastream.connectors")
_pyflink_ds_conn_k    = _make_module("pyflink.datastream.connectors.kafka")
_pyflink_ds_functions = _make_module("pyflink.datastream.functions")
_pyflink_ds_state     = _make_module("pyflink.datastream.state")
_pyflink_ds_window    = _make_module("pyflink.datastream.window")
_pyflink_table        = _make_module("pyflink.table")
_pyflink_table_udf    = _make_module("pyflink.table.udf")

# pyflink.common exports
_pyflink_common.Time              = _Stub
_pyflink_common.WatermarkStrategy = _Stub
_pyflink_common.Duration          = _Stub

# pyflink.common.watermark_strategy
_pyflink_common_ws.TimestampAssigner = _Stub

# pyflink.common.serialization
_pyflink_common_ser.SimpleStringSchema = _Stub

# pyflink.common.typeinfo — use real Types class
_pyflink_common_ti.Types = Types

# pyflink.datastream exports
_pyflink_ds.StreamExecutionEnvironment = _Stub
_pyflink_ds.RuntimeExecutionMode       = _Stub

# pyflink.datastream.connectors.kafka
_pyflink_ds_conn_k.KafkaSource                    = _Stub
_pyflink_ds_conn_k.KafkaOffsetsInitializer        = _Stub
_pyflink_ds_conn_k.KafkaOffsetResetStrategy       = _Stub
_pyflink_ds_conn_k.KafkaSink                      = _Stub
_pyflink_ds_conn_k.KafkaRecordSerializationSchema = _Stub

# pyflink.datastream.functions — use real base classes
_pyflink_ds_functions.KeyedBroadcastProcessFunction = KeyedBroadcastProcessFunction
_pyflink_ds_functions.KeyedProcessFunction          = KeyedProcessFunction
_pyflink_ds_functions.BroadcastProcessFunction      = BroadcastProcessFunction
_pyflink_ds_functions.RuntimeContext                = RuntimeContext
_pyflink_ds_functions.AggregateFunction             = AggregateFunction
_pyflink_ds_functions.ProcessWindowFunction         = ProcessWindowFunction

# pyflink.datastream.state — use real descriptor classes
_pyflink_ds_state.ValueStateDescriptor = ValueStateDescriptor
_pyflink_ds_state.MapStateDescriptor   = MapStateDescriptor

# pyflink.datastream.window
_pyflink_ds_window.TumblingEventTimeWindows = _Stub

# ─────────────────────────────────────────────────────────────────
#  confluent_kafka — stub Producer used by payment_producer.py
# ─────────────────────────────────────────────────────────────────
_confluent_kafka = _make_module("confluent_kafka")
_confluent_kafka.Producer = MagicMock

# ─────────────────────────────────────────────────────────────────
#  faker — stub if not installed locally
# ─────────────────────────────────────────────────────────────────
try:
    import faker  # noqa: F401
except ImportError:
    _faker = _make_module("faker")
    _faker.Faker = MagicMock
