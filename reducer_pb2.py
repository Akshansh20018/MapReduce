# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: reducer.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rreducer.proto\"-\n\x08\x43\x65ntroid\x12\x0b\n\x03idx\x18\x01 \x01(\x05\x12\t\n\x01x\x18\x02 \x01(\x01\x12\t\n\x01y\x18\x03 \x01(\x01\"\x1c\n\x04\x43\x65nt\x12\t\n\x01x\x18\x01 \x01(\x01\x12\t\n\x01y\x18\x02 \x01(\x01\"g\n\x12ShuffleSortRequest\x12\x12\n\ncentroidID\x18\x01 \x01(\x05\x12\x12\n\nnumMappers\x18\x02 \x01(\x05\x12\x14\n\x0cmapAddresses\x18\x03 \x03(\x05\x12\x13\n\x0bnumReducers\x18\x04 \x01(\x05\"V\n\x0fReducerResponse\x12\x12\n\ncentroidID\x18\x01 \x01(\x05\x12\x1f\n\x10updatedCentroids\x18\x02 \x01(\x0b\x32\x05.Cent\x12\x0e\n\x06status\x18\x03 \x01(\x08\x32\x42\n\x07Reducer\x12\x37\n\x0eShuffleAndSort\x12\x13.ShuffleSortRequest\x1a\x10.ReducerResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'reducer_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_CENTROID']._serialized_start=17
  _globals['_CENTROID']._serialized_end=62
  _globals['_CENT']._serialized_start=64
  _globals['_CENT']._serialized_end=92
  _globals['_SHUFFLESORTREQUEST']._serialized_start=94
  _globals['_SHUFFLESORTREQUEST']._serialized_end=197
  _globals['_REDUCERRESPONSE']._serialized_start=199
  _globals['_REDUCERRESPONSE']._serialized_end=285
  _globals['_REDUCER']._serialized_start=287
  _globals['_REDUCER']._serialized_end=353
# @@protoc_insertion_point(module_scope)
