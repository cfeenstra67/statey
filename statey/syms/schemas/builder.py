from statey.syms import types
from statey.syms.schemas import base


Integer = base.ValueSchemaFactory(types.IntegerType)

Boolean = base.ValueSchemaFactory(types.BooleanType)

String = base.ValueSchemaFactory(types.StringType)

Float = base.ValueSchemaFactory(types.FloatType)

Array = base.ArraySchemaFactory()

Struct = base.StructSchemaFactory()
