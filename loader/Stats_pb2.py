# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: src/Stats.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)




DESCRIPTOR = _descriptor.FileDescriptor(
  name='Dropbox (Personale)/master_THESIS/src/Stats.proto',
  package='',
  serialized_pb='\n1Dropbox (Personale)/master_THESIS/src/Stats.proto\"P\n\x05Table\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\x0c\n\x04size\x18\x02 \x01(\x05\x12\x11\n\tisComplex\x18\x03 \x01(\x08\x12\x18\n\x10\x64istinctSubjects\x18\x04 \x01(\x05\"U\n\x05Graph\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04size\x18\x02 \x01(\x05\x12\x18\n\x10\x64istinctSubjects\x18\x03 \x01(\x05\x12\x16\n\x06tables\x18\x04 \x03(\x0b\x32\x06.Table')




_TABLE = _descriptor.Descriptor(
  name='Table',
  full_name='Table',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='Table.name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='size', full_name='Table.size', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='isComplex', full_name='Table.isComplex', index=2,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='distinctSubjects', full_name='Table.distinctSubjects', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=53,
  serialized_end=133,
)


_GRAPH = _descriptor.Descriptor(
  name='Graph',
  full_name='Graph',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='Graph.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='size', full_name='Graph.size', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='distinctSubjects', full_name='Graph.distinctSubjects', index=2,
      number=3, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='tables', full_name='Graph.tables', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=135,
  serialized_end=220,
)

_GRAPH.fields_by_name['tables'].message_type = _TABLE
DESCRIPTOR.message_types_by_name['Table'] = _TABLE
DESCRIPTOR.message_types_by_name['Graph'] = _GRAPH

class Table(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _TABLE

  # @@protoc_insertion_point(class_scope:Table)

class Graph(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GRAPH

  # @@protoc_insertion_point(class_scope:Graph)


# @@protoc_insertion_point(module_scope)