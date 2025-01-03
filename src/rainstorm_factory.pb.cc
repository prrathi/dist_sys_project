// Generated by the protocol buffer compiler.  DO NOT EDIT!
// NO CHECKED-IN PROTOBUF GENCODE
// source: rainstorm_factory.proto
// Protobuf C++ Version: 5.27.2

#include "rainstorm_factory.pb.h"

#include <algorithm>
#include <type_traits>
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/generated_message_tctable_impl.h"
#include "google/protobuf/extension_set.h"
#include "google/protobuf/wire_format_lite.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/generated_message_reflection.h"
#include "google/protobuf/reflection_ops.h"
#include "google/protobuf/wire_format.h"
// @@protoc_insertion_point(includes)

// Must be included last.
#include "google/protobuf/port_def.inc"
PROTOBUF_PRAGMA_INIT_SEG
namespace _pb = ::google::protobuf;
namespace _pbi = ::google::protobuf::internal;
namespace _fl = ::google::protobuf::internal::field_layout;
namespace rainstorm_factory {

inline constexpr ServerRequest::Impl_::Impl_(
    ::_pbi::ConstantInitialized) noexcept
      : port_{0},
        node_type_{static_cast< ::rainstorm_factory::NodeType >(0)},
        _cached_size_{0} {}

template <typename>
PROTOBUF_CONSTEXPR ServerRequest::ServerRequest(::_pbi::ConstantInitialized)
    : _impl_(::_pbi::ConstantInitialized()) {}
struct ServerRequestDefaultTypeInternal {
  PROTOBUF_CONSTEXPR ServerRequestDefaultTypeInternal() : _instance(::_pbi::ConstantInitialized{}) {}
  ~ServerRequestDefaultTypeInternal() {}
  union {
    ServerRequest _instance;
  };
};

PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT
    PROTOBUF_ATTRIBUTE_INIT_PRIORITY1 ServerRequestDefaultTypeInternal _ServerRequest_default_instance_;

inline constexpr OperationStatus::Impl_::Impl_(
    ::_pbi::ConstantInitialized) noexcept
      : message_(
            &::google::protobuf::internal::fixed_address_empty_string,
            ::_pbi::ConstantInitialized()),
        status_{static_cast< ::rainstorm_factory::StatusCode >(0)},
        _cached_size_{0} {}

template <typename>
PROTOBUF_CONSTEXPR OperationStatus::OperationStatus(::_pbi::ConstantInitialized)
    : _impl_(::_pbi::ConstantInitialized()) {}
struct OperationStatusDefaultTypeInternal {
  PROTOBUF_CONSTEXPR OperationStatusDefaultTypeInternal() : _instance(::_pbi::ConstantInitialized{}) {}
  ~OperationStatusDefaultTypeInternal() {}
  union {
    OperationStatus _instance;
  };
};

PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT
    PROTOBUF_ATTRIBUTE_INIT_PRIORITY1 OperationStatusDefaultTypeInternal _OperationStatus_default_instance_;
}  // namespace rainstorm_factory
static const ::_pb::EnumDescriptor* file_level_enum_descriptors_rainstorm_5ffactory_2eproto[2];
static constexpr const ::_pb::ServiceDescriptor**
    file_level_service_descriptors_rainstorm_5ffactory_2eproto = nullptr;
const ::uint32_t
    TableStruct_rainstorm_5ffactory_2eproto::offsets[] ABSL_ATTRIBUTE_SECTION_VARIABLE(
        protodesc_cold) = {
        ~0u,  // no _has_bits_
        PROTOBUF_FIELD_OFFSET(::rainstorm_factory::OperationStatus, _internal_metadata_),
        ~0u,  // no _extensions_
        ~0u,  // no _oneof_case_
        ~0u,  // no _weak_field_map_
        ~0u,  // no _inlined_string_donated_
        ~0u,  // no _split_
        ~0u,  // no sizeof(Split)
        PROTOBUF_FIELD_OFFSET(::rainstorm_factory::OperationStatus, _impl_.status_),
        PROTOBUF_FIELD_OFFSET(::rainstorm_factory::OperationStatus, _impl_.message_),
        ~0u,  // no _has_bits_
        PROTOBUF_FIELD_OFFSET(::rainstorm_factory::ServerRequest, _internal_metadata_),
        ~0u,  // no _extensions_
        ~0u,  // no _oneof_case_
        ~0u,  // no _weak_field_map_
        ~0u,  // no _inlined_string_donated_
        ~0u,  // no _split_
        ~0u,  // no sizeof(Split)
        PROTOBUF_FIELD_OFFSET(::rainstorm_factory::ServerRequest, _impl_.port_),
        PROTOBUF_FIELD_OFFSET(::rainstorm_factory::ServerRequest, _impl_.node_type_),
};

static const ::_pbi::MigrationSchema
    schemas[] ABSL_ATTRIBUTE_SECTION_VARIABLE(protodesc_cold) = {
        {0, -1, -1, sizeof(::rainstorm_factory::OperationStatus)},
        {10, -1, -1, sizeof(::rainstorm_factory::ServerRequest)},
};
static const ::_pb::Message* const file_default_instances[] = {
    &::rainstorm_factory::_OperationStatus_default_instance_._instance,
    &::rainstorm_factory::_ServerRequest_default_instance_._instance,
};
const char descriptor_table_protodef_rainstorm_5ffactory_2eproto[] ABSL_ATTRIBUTE_SECTION_VARIABLE(
    protodesc_cold) = {
    "\n\027rainstorm_factory.proto\022\021rainstorm_fac"
    "tory\"Q\n\017OperationStatus\022-\n\006status\030\001 \001(\0162"
    "\035.rainstorm_factory.StatusCode\022\017\n\007messag"
    "e\030\002 \001(\t\"M\n\rServerRequest\022\014\n\004port\030\001 \001(\005\022."
    "\n\tnode_type\030\002 \001(\0162\033.rainstorm_factory.No"
    "deType*I\n\nStatusCode\022\013\n\007SUCCESS\020\000\022\013\n\007INV"
    "ALID\020\001\022\r\n\tNOT_FOUND\020\002\022\022\n\016ALREADY_EXISTS\020"
    "\003*(\n\010NodeType\022\014\n\010SRC_NODE\020\000\022\016\n\nSTAGE_NOD"
    "E\020\0012\305\001\n\027RainstormFactoryService\022T\n\014Creat"
    "eServer\022 .rainstorm_factory.ServerReques"
    "t\032\".rainstorm_factory.OperationStatus\022T\n"
    "\014RemoveServer\022 .rainstorm_factory.Server"
    "Request\032\".rainstorm_factory.OperationSta"
    "tusb\006proto3"
};
static ::absl::once_flag descriptor_table_rainstorm_5ffactory_2eproto_once;
PROTOBUF_CONSTINIT const ::_pbi::DescriptorTable descriptor_table_rainstorm_5ffactory_2eproto = {
    false,
    false,
    531,
    descriptor_table_protodef_rainstorm_5ffactory_2eproto,
    "rainstorm_factory.proto",
    &descriptor_table_rainstorm_5ffactory_2eproto_once,
    nullptr,
    0,
    2,
    schemas,
    file_default_instances,
    TableStruct_rainstorm_5ffactory_2eproto::offsets,
    file_level_enum_descriptors_rainstorm_5ffactory_2eproto,
    file_level_service_descriptors_rainstorm_5ffactory_2eproto,
};
namespace rainstorm_factory {
const ::google::protobuf::EnumDescriptor* StatusCode_descriptor() {
  ::google::protobuf::internal::AssignDescriptors(&descriptor_table_rainstorm_5ffactory_2eproto);
  return file_level_enum_descriptors_rainstorm_5ffactory_2eproto[0];
}
PROTOBUF_CONSTINIT const uint32_t StatusCode_internal_data_[] = {
    262144u, 0u, };
bool StatusCode_IsValid(int value) {
  return 0 <= value && value <= 3;
}
const ::google::protobuf::EnumDescriptor* NodeType_descriptor() {
  ::google::protobuf::internal::AssignDescriptors(&descriptor_table_rainstorm_5ffactory_2eproto);
  return file_level_enum_descriptors_rainstorm_5ffactory_2eproto[1];
}
PROTOBUF_CONSTINIT const uint32_t NodeType_internal_data_[] = {
    131072u, 0u, };
bool NodeType_IsValid(int value) {
  return 0 <= value && value <= 1;
}
// ===================================================================

class OperationStatus::_Internal {
 public:
};

OperationStatus::OperationStatus(::google::protobuf::Arena* arena)
    : ::google::protobuf::Message(arena) {
  SharedCtor(arena);
  // @@protoc_insertion_point(arena_constructor:rainstorm_factory.OperationStatus)
}
inline PROTOBUF_NDEBUG_INLINE OperationStatus::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility, ::google::protobuf::Arena* arena,
    const Impl_& from, const ::rainstorm_factory::OperationStatus& from_msg)
      : message_(arena, from.message_),
        _cached_size_{0} {}

OperationStatus::OperationStatus(
    ::google::protobuf::Arena* arena,
    const OperationStatus& from)
    : ::google::protobuf::Message(arena) {
  OperationStatus* const _this = this;
  (void)_this;
  _internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(
      from._internal_metadata_);
  new (&_impl_) Impl_(internal_visibility(), arena, from._impl_, from);
  _impl_.status_ = from._impl_.status_;

  // @@protoc_insertion_point(copy_constructor:rainstorm_factory.OperationStatus)
}
inline PROTOBUF_NDEBUG_INLINE OperationStatus::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility,
    ::google::protobuf::Arena* arena)
      : message_(arena),
        _cached_size_{0} {}

inline void OperationStatus::SharedCtor(::_pb::Arena* arena) {
  new (&_impl_) Impl_(internal_visibility(), arena);
  _impl_.status_ = {};
}
OperationStatus::~OperationStatus() {
  // @@protoc_insertion_point(destructor:rainstorm_factory.OperationStatus)
  _internal_metadata_.Delete<::google::protobuf::UnknownFieldSet>();
  SharedDtor();
}
inline void OperationStatus::SharedDtor() {
  ABSL_DCHECK(GetArena() == nullptr);
  _impl_.message_.Destroy();
  _impl_.~Impl_();
}

const ::google::protobuf::MessageLite::ClassData*
OperationStatus::GetClassData() const {
  PROTOBUF_CONSTINIT static const ::google::protobuf::MessageLite::
      ClassDataFull _data_ = {
          {
              &_table_.header,
              nullptr,  // OnDemandRegisterArenaDtor
              nullptr,  // IsInitialized
              PROTOBUF_FIELD_OFFSET(OperationStatus, _impl_._cached_size_),
              false,
          },
          &OperationStatus::MergeImpl,
          &OperationStatus::kDescriptorMethods,
          &descriptor_table_rainstorm_5ffactory_2eproto,
          nullptr,  // tracker
      };
  ::google::protobuf::internal::PrefetchToLocalCache(&_data_);
  ::google::protobuf::internal::PrefetchToLocalCache(_data_.tc_table);
  return _data_.base();
}
PROTOBUF_CONSTINIT PROTOBUF_ATTRIBUTE_INIT_PRIORITY1
const ::_pbi::TcParseTable<1, 2, 0, 49, 2> OperationStatus::_table_ = {
  {
    0,  // no _has_bits_
    0, // no _extensions_
    2, 8,  // max_field_number, fast_idx_mask
    offsetof(decltype(_table_), field_lookup_table),
    4294967292,  // skipmap
    offsetof(decltype(_table_), field_entries),
    2,  // num_field_entries
    0,  // num_aux_entries
    offsetof(decltype(_table_), field_names),  // no aux_entries
    &_OperationStatus_default_instance_._instance,
    nullptr,  // post_loop_handler
    ::_pbi::TcParser::GenericFallback,  // fallback
    #ifdef PROTOBUF_PREFETCH_PARSE_TABLE
    ::_pbi::TcParser::GetTable<::rainstorm_factory::OperationStatus>(),  // to_prefetch
    #endif  // PROTOBUF_PREFETCH_PARSE_TABLE
  }, {{
    // string message = 2;
    {::_pbi::TcParser::FastUS1,
     {18, 63, 0, PROTOBUF_FIELD_OFFSET(OperationStatus, _impl_.message_)}},
    // .rainstorm_factory.StatusCode status = 1;
    {::_pbi::TcParser::SingularVarintNoZag1<::uint32_t, offsetof(OperationStatus, _impl_.status_), 63>(),
     {8, 63, 0, PROTOBUF_FIELD_OFFSET(OperationStatus, _impl_.status_)}},
  }}, {{
    65535, 65535
  }}, {{
    // .rainstorm_factory.StatusCode status = 1;
    {PROTOBUF_FIELD_OFFSET(OperationStatus, _impl_.status_), 0, 0,
    (0 | ::_fl::kFcSingular | ::_fl::kOpenEnum)},
    // string message = 2;
    {PROTOBUF_FIELD_OFFSET(OperationStatus, _impl_.message_), 0, 0,
    (0 | ::_fl::kFcSingular | ::_fl::kUtf8String | ::_fl::kRepAString)},
  }},
  // no aux_entries
  {{
    "\41\0\7\0\0\0\0\0"
    "rainstorm_factory.OperationStatus"
    "message"
  }},
};

PROTOBUF_NOINLINE void OperationStatus::Clear() {
// @@protoc_insertion_point(message_clear_start:rainstorm_factory.OperationStatus)
  ::google::protobuf::internal::TSanWrite(&_impl_);
  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  _impl_.message_.ClearToEmpty();
  _impl_.status_ = 0;
  _internal_metadata_.Clear<::google::protobuf::UnknownFieldSet>();
}

::uint8_t* OperationStatus::_InternalSerialize(
    ::uint8_t* target,
    ::google::protobuf::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:rainstorm_factory.OperationStatus)
  ::uint32_t cached_has_bits = 0;
  (void)cached_has_bits;

  // .rainstorm_factory.StatusCode status = 1;
  if (this->_internal_status() != 0) {
    target = stream->EnsureSpace(target);
    target = ::_pbi::WireFormatLite::WriteEnumToArray(
        1, this->_internal_status(), target);
  }

  // string message = 2;
  if (!this->_internal_message().empty()) {
    const std::string& _s = this->_internal_message();
    ::google::protobuf::internal::WireFormatLite::VerifyUtf8String(
        _s.data(), static_cast<int>(_s.length()), ::google::protobuf::internal::WireFormatLite::SERIALIZE, "rainstorm_factory.OperationStatus.message");
    target = stream->WriteStringMaybeAliased(2, _s, target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target =
        ::_pbi::WireFormat::InternalSerializeUnknownFieldsToArray(
            _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:rainstorm_factory.OperationStatus)
  return target;
}

::size_t OperationStatus::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:rainstorm_factory.OperationStatus)
  ::size_t total_size = 0;

  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  ::_pbi::Prefetch5LinesFrom7Lines(reinterpret_cast<const void*>(this));
  // string message = 2;
  if (!this->_internal_message().empty()) {
    total_size += 1 + ::google::protobuf::internal::WireFormatLite::StringSize(
                                    this->_internal_message());
  }

  // .rainstorm_factory.StatusCode status = 1;
  if (this->_internal_status() != 0) {
    total_size += 1 +
                  ::_pbi::WireFormatLite::EnumSize(this->_internal_status());
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_impl_._cached_size_);
}


void OperationStatus::MergeImpl(::google::protobuf::MessageLite& to_msg, const ::google::protobuf::MessageLite& from_msg) {
  auto* const _this = static_cast<OperationStatus*>(&to_msg);
  auto& from = static_cast<const OperationStatus&>(from_msg);
  // @@protoc_insertion_point(class_specific_merge_from_start:rainstorm_factory.OperationStatus)
  ABSL_DCHECK_NE(&from, _this);
  ::uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  if (!from._internal_message().empty()) {
    _this->_internal_set_message(from._internal_message());
  }
  if (from._internal_status() != 0) {
    _this->_impl_.status_ = from._impl_.status_;
  }
  _this->_internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(from._internal_metadata_);
}

void OperationStatus::CopyFrom(const OperationStatus& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:rainstorm_factory.OperationStatus)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}


void OperationStatus::InternalSwap(OperationStatus* PROTOBUF_RESTRICT other) {
  using std::swap;
  auto* arena = GetArena();
  ABSL_DCHECK_EQ(arena, other->GetArena());
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  ::_pbi::ArenaStringPtr::InternalSwap(&_impl_.message_, &other->_impl_.message_, arena);
  swap(_impl_.status_, other->_impl_.status_);
}

::google::protobuf::Metadata OperationStatus::GetMetadata() const {
  return ::google::protobuf::Message::GetMetadataImpl(GetClassData()->full());
}
// ===================================================================

class ServerRequest::_Internal {
 public:
};

ServerRequest::ServerRequest(::google::protobuf::Arena* arena)
    : ::google::protobuf::Message(arena) {
  SharedCtor(arena);
  // @@protoc_insertion_point(arena_constructor:rainstorm_factory.ServerRequest)
}
ServerRequest::ServerRequest(
    ::google::protobuf::Arena* arena, const ServerRequest& from)
    : ServerRequest(arena) {
  MergeFrom(from);
}
inline PROTOBUF_NDEBUG_INLINE ServerRequest::Impl_::Impl_(
    ::google::protobuf::internal::InternalVisibility visibility,
    ::google::protobuf::Arena* arena)
      : _cached_size_{0} {}

inline void ServerRequest::SharedCtor(::_pb::Arena* arena) {
  new (&_impl_) Impl_(internal_visibility(), arena);
  ::memset(reinterpret_cast<char *>(&_impl_) +
               offsetof(Impl_, port_),
           0,
           offsetof(Impl_, node_type_) -
               offsetof(Impl_, port_) +
               sizeof(Impl_::node_type_));
}
ServerRequest::~ServerRequest() {
  // @@protoc_insertion_point(destructor:rainstorm_factory.ServerRequest)
  _internal_metadata_.Delete<::google::protobuf::UnknownFieldSet>();
  SharedDtor();
}
inline void ServerRequest::SharedDtor() {
  ABSL_DCHECK(GetArena() == nullptr);
  _impl_.~Impl_();
}

const ::google::protobuf::MessageLite::ClassData*
ServerRequest::GetClassData() const {
  PROTOBUF_CONSTINIT static const ::google::protobuf::MessageLite::
      ClassDataFull _data_ = {
          {
              &_table_.header,
              nullptr,  // OnDemandRegisterArenaDtor
              nullptr,  // IsInitialized
              PROTOBUF_FIELD_OFFSET(ServerRequest, _impl_._cached_size_),
              false,
          },
          &ServerRequest::MergeImpl,
          &ServerRequest::kDescriptorMethods,
          &descriptor_table_rainstorm_5ffactory_2eproto,
          nullptr,  // tracker
      };
  ::google::protobuf::internal::PrefetchToLocalCache(&_data_);
  ::google::protobuf::internal::PrefetchToLocalCache(_data_.tc_table);
  return _data_.base();
}
PROTOBUF_CONSTINIT PROTOBUF_ATTRIBUTE_INIT_PRIORITY1
const ::_pbi::TcParseTable<1, 2, 0, 0, 2> ServerRequest::_table_ = {
  {
    0,  // no _has_bits_
    0, // no _extensions_
    2, 8,  // max_field_number, fast_idx_mask
    offsetof(decltype(_table_), field_lookup_table),
    4294967292,  // skipmap
    offsetof(decltype(_table_), field_entries),
    2,  // num_field_entries
    0,  // num_aux_entries
    offsetof(decltype(_table_), field_names),  // no aux_entries
    &_ServerRequest_default_instance_._instance,
    nullptr,  // post_loop_handler
    ::_pbi::TcParser::GenericFallback,  // fallback
    #ifdef PROTOBUF_PREFETCH_PARSE_TABLE
    ::_pbi::TcParser::GetTable<::rainstorm_factory::ServerRequest>(),  // to_prefetch
    #endif  // PROTOBUF_PREFETCH_PARSE_TABLE
  }, {{
    // .rainstorm_factory.NodeType node_type = 2;
    {::_pbi::TcParser::SingularVarintNoZag1<::uint32_t, offsetof(ServerRequest, _impl_.node_type_), 63>(),
     {16, 63, 0, PROTOBUF_FIELD_OFFSET(ServerRequest, _impl_.node_type_)}},
    // int32 port = 1;
    {::_pbi::TcParser::SingularVarintNoZag1<::uint32_t, offsetof(ServerRequest, _impl_.port_), 63>(),
     {8, 63, 0, PROTOBUF_FIELD_OFFSET(ServerRequest, _impl_.port_)}},
  }}, {{
    65535, 65535
  }}, {{
    // int32 port = 1;
    {PROTOBUF_FIELD_OFFSET(ServerRequest, _impl_.port_), 0, 0,
    (0 | ::_fl::kFcSingular | ::_fl::kInt32)},
    // .rainstorm_factory.NodeType node_type = 2;
    {PROTOBUF_FIELD_OFFSET(ServerRequest, _impl_.node_type_), 0, 0,
    (0 | ::_fl::kFcSingular | ::_fl::kOpenEnum)},
  }},
  // no aux_entries
  {{
  }},
};

PROTOBUF_NOINLINE void ServerRequest::Clear() {
// @@protoc_insertion_point(message_clear_start:rainstorm_factory.ServerRequest)
  ::google::protobuf::internal::TSanWrite(&_impl_);
  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  ::memset(&_impl_.port_, 0, static_cast<::size_t>(
      reinterpret_cast<char*>(&_impl_.node_type_) -
      reinterpret_cast<char*>(&_impl_.port_)) + sizeof(_impl_.node_type_));
  _internal_metadata_.Clear<::google::protobuf::UnknownFieldSet>();
}

::uint8_t* ServerRequest::_InternalSerialize(
    ::uint8_t* target,
    ::google::protobuf::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:rainstorm_factory.ServerRequest)
  ::uint32_t cached_has_bits = 0;
  (void)cached_has_bits;

  // int32 port = 1;
  if (this->_internal_port() != 0) {
    target = ::google::protobuf::internal::WireFormatLite::
        WriteInt32ToArrayWithField<1>(
            stream, this->_internal_port(), target);
  }

  // .rainstorm_factory.NodeType node_type = 2;
  if (this->_internal_node_type() != 0) {
    target = stream->EnsureSpace(target);
    target = ::_pbi::WireFormatLite::WriteEnumToArray(
        2, this->_internal_node_type(), target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target =
        ::_pbi::WireFormat::InternalSerializeUnknownFieldsToArray(
            _internal_metadata_.unknown_fields<::google::protobuf::UnknownFieldSet>(::google::protobuf::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:rainstorm_factory.ServerRequest)
  return target;
}

::size_t ServerRequest::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:rainstorm_factory.ServerRequest)
  ::size_t total_size = 0;

  ::uint32_t cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  ::_pbi::Prefetch5LinesFrom7Lines(reinterpret_cast<const void*>(this));
  // int32 port = 1;
  if (this->_internal_port() != 0) {
    total_size += ::_pbi::WireFormatLite::Int32SizePlusOne(
        this->_internal_port());
  }

  // .rainstorm_factory.NodeType node_type = 2;
  if (this->_internal_node_type() != 0) {
    total_size += 1 +
                  ::_pbi::WireFormatLite::EnumSize(this->_internal_node_type());
  }

  return MaybeComputeUnknownFieldsSize(total_size, &_impl_._cached_size_);
}


void ServerRequest::MergeImpl(::google::protobuf::MessageLite& to_msg, const ::google::protobuf::MessageLite& from_msg) {
  auto* const _this = static_cast<ServerRequest*>(&to_msg);
  auto& from = static_cast<const ServerRequest&>(from_msg);
  // @@protoc_insertion_point(class_specific_merge_from_start:rainstorm_factory.ServerRequest)
  ABSL_DCHECK_NE(&from, _this);
  ::uint32_t cached_has_bits = 0;
  (void) cached_has_bits;

  if (from._internal_port() != 0) {
    _this->_impl_.port_ = from._impl_.port_;
  }
  if (from._internal_node_type() != 0) {
    _this->_impl_.node_type_ = from._impl_.node_type_;
  }
  _this->_internal_metadata_.MergeFrom<::google::protobuf::UnknownFieldSet>(from._internal_metadata_);
}

void ServerRequest::CopyFrom(const ServerRequest& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:rainstorm_factory.ServerRequest)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}


void ServerRequest::InternalSwap(ServerRequest* PROTOBUF_RESTRICT other) {
  using std::swap;
  _internal_metadata_.InternalSwap(&other->_internal_metadata_);
  ::google::protobuf::internal::memswap<
      PROTOBUF_FIELD_OFFSET(ServerRequest, _impl_.node_type_)
      + sizeof(ServerRequest::_impl_.node_type_)
      - PROTOBUF_FIELD_OFFSET(ServerRequest, _impl_.port_)>(
          reinterpret_cast<char*>(&_impl_.port_),
          reinterpret_cast<char*>(&other->_impl_.port_));
}

::google::protobuf::Metadata ServerRequest::GetMetadata() const {
  return ::google::protobuf::Message::GetMetadataImpl(GetClassData()->full());
}
// @@protoc_insertion_point(namespace_scope)
}  // namespace rainstorm_factory
namespace google {
namespace protobuf {
}  // namespace protobuf
}  // namespace google
// @@protoc_insertion_point(global_scope)
PROTOBUF_ATTRIBUTE_INIT_PRIORITY2 static ::std::false_type
    _static_init2_ PROTOBUF_UNUSED =
        (::_pbi::AddDescriptors(&descriptor_table_rainstorm_5ffactory_2eproto),
         ::std::false_type{});
#include "google/protobuf/port_undef.inc"
