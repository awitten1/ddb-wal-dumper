
#include <_types/_uint8_t.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include "duckdb.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/serializer/serialization_traits.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/common/serializer/encoding_util.hpp"
#include <fcntl.h>
#include <type_traits>
#include <nlohmann/json.hpp>
#include <optional>
#include <vector>

#define ASSERT(ret) \
    if (!(ret)) {           \
        std::cerr << "failed assertion " << #ret << std::endl; \
        std::abort();        \
    }

auto OpenWal(const std::string& wal_path) {
    struct stat st;
    stat(wal_path.c_str(), &st);
    int fd = open(wal_path.c_str(), O_RDONLY);
    ASSERT(fd > 0);
    uint8_t* buf = (uint8_t*)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    ASSERT(buf != MAP_FAILED);
    auto rel = [st](uint8_t* buf) {
        munmap(buf, st.st_size);
    };

    std::unique_ptr<uint8_t, decltype(rel)> b(buf, rel);
    madvise(buf, st.st_size, MADV_SEQUENTIAL);

    return std::make_pair(std::move(b), st.st_size);
}

void ReadVersion(duckdb::BinaryDeserializer& deserializer) {
    deserializer.Begin();
    // https://github.com/duckdb/duckdb/blob/v1.1.3/src/storage/write_ahead_log.cpp#L164
    auto wal_type = deserializer.ReadProperty<duckdb::WALType>(100, "wal_type");
    ASSERT(wal_type == duckdb::WALType::WAL_VERSION);

    auto wal_version_number = deserializer.ReadProperty<duckdb::idx_t>(101, "version");
    ASSERT(wal_version_number == 2);
    deserializer.End();
}

// Consumes a buffer.  Non-owning.
class BufferStream : public duckdb::ReadStream {
    uint8_t* buffer_;
    const uint8_t* end_ptr_;
    const uint8_t* start_ptr_;
public:

    BufferStream(uint8_t* buffer, size_t sz) : buffer_(buffer), end_ptr_(buffer_ + sz), start_ptr_(buffer) {}
    void ReadData(uint8_t* dst, idx_t read_size) override {
        memcpy(dst, buffer_, read_size);
        buffer_ += read_size;
    }

    bool Done() {
        return buffer_ == end_ptr_;
    }

    uint8_t* GetBuffer() {
        return buffer_;
    }

    size_t GetRemainder() {
        return end_ptr_ - buffer_;
    }

    size_t GetOffset() {
        return buffer_ - start_ptr_;
    }
};

struct CreateTableInfo {
    CreateTableInfo(duckdb::WALType wal_type_, size_t offset) : file_offset(offset) {
        wal_type = duckdb::EnumUtil::ToString(wal_type_);
    }
    std::string table;
    std::string query;
    std::string schema;
    std::vector<std::string> columns;
    std::string wal_type;
    size_t file_offset;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(CreateTableInfo, table, schema, query, columns, wal_type, file_offset);
};

struct DropTableInfo {
    DropTableInfo(duckdb::WALType wal_type_, size_t offset) : file_offset(offset) {
        wal_type = duckdb::EnumUtil::ToString(wal_type_);
    }
    std::string table;
    std::string schema;
    std::string wal_type;
    size_t file_offset;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(DropTableInfo, table, schema, wal_type, file_offset);
};



void ParseCreateTable(duckdb::BinaryDeserializer& deserializer, duckdb::WALType wal_type, size_t file_offset) {
    deserializer.Begin();
    auto info = deserializer.ReadProperty<duckdb::unique_ptr<duckdb::CreateInfo>>(101, "table");

    auto& create_table_info = dynamic_cast<duckdb::CreateTableInfo&>(*info);
    CreateTableInfo ct{wal_type, file_offset};
    ct.table = create_table_info.table;
    ct.columns = create_table_info.columns.GetColumnNames();
    ct.schema = create_table_info.schema;
    if (create_table_info.query) {
        ct.query = create_table_info.query->ToString();
    }
    nlohmann::json j = ct;
    std::cout << j.dump() << std::endl;

    deserializer.End();
}

void ParseDropTable(duckdb::BinaryDeserializer& deserializer, duckdb::WALType wal_type, size_t file_offset) {
    duckdb::DropInfo info;

	info.type = duckdb::CatalogType::TABLE_ENTRY;
	info.schema = deserializer.ReadProperty<std::string>(101, "schema");
	info.name = deserializer.ReadProperty<std::string>(102, "name");

    DropTableInfo dt{wal_type, file_offset};
    dt.table = info.name;
    dt.schema = info.schema;
    nlohmann::json j = dt;
    std::cout << j.dump() << std::endl;

    deserializer.End();
}

void ParseInsertTuple(duckdb::BinaryDeserializer& deserializer, duckdb::WALType wal_type, size_t file_offset) {
    duckdb::DataChunk chunk;
	deserializer.ReadObject(101, "chunk", [&](duckdb::Deserializer &object) { chunk.Deserialize(object); });
    std::cout << chunk.ColumnCount() << std::endl;
    deserializer.End();
}

void ParseWalRecord(std::unique_ptr<uint8_t[]> wal_buf, size_t sz, size_t offset) {
    BufferStream stream(wal_buf.get(), sz);
    duckdb::BinaryDeserializer deserializer(stream);
    auto wal_enum_type = deserializer.ReadProperty<duckdb::WALType>(100, "wal_type");

    switch (wal_enum_type) {
    case duckdb::WALType::INVALID:
        throw std::runtime_error{"invalid wal type"};
        break;
    case duckdb::WALType::CREATE_TABLE:
        ParseCreateTable(deserializer, wal_enum_type, offset);
        break;
    case duckdb::WALType::DROP_TABLE:
        ParseDropTable(deserializer, wal_enum_type, offset);
        break;
    case duckdb::WALType::CREATE_SCHEMA:
    
    break;
    case duckdb::WALType::DROP_SCHEMA:
    
    break;
    case duckdb::WALType::CREATE_VIEW:
    
    break;
    case duckdb::WALType::DROP_VIEW:
    
    break;
    case duckdb::WALType::CREATE_SEQUENCE:
    
    break;
    case duckdb::WALType::DROP_SEQUENCE:
    
    break;
    case duckdb::WALType::SEQUENCE_VALUE:
    
    break;
    case duckdb::WALType::CREATE_MACRO:
    
    break;
    case duckdb::WALType::DROP_MACRO:
    
    break;
    case duckdb::WALType::CREATE_TYPE:
    
    break;
    case duckdb::WALType::DROP_TYPE:
    
    break;
    case duckdb::WALType::ALTER_INFO:
    
    break;
    case duckdb::WALType::CREATE_TABLE_MACRO:
    
    break;
    case duckdb::WALType::DROP_TABLE_MACRO:
    
    break;
    case duckdb::WALType::CREATE_INDEX:
    
    break;
    case duckdb::WALType::DROP_INDEX:
    
    break;
    case duckdb::WALType::USE_TABLE:
    
    break;
    case duckdb::WALType::INSERT_TUPLE:
        ParseInsertTuple(deserializer, wal_enum_type, offset);
        break;
    case duckdb::WALType::DELETE_TUPLE:
    
    break;
    case duckdb::WALType::UPDATE_TUPLE:
    
    break;
    case duckdb::WALType::ROW_GROUP_DATA:
    
    break;
    case duckdb::WALType::WAL_VERSION:
    
    break;
    case duckdb::WALType::CHECKPOINT:
    
    break;
    case duckdb::WALType::WAL_FLUSH:
    
    break;
    }
}

void ScanWal(uint8_t* buf, uint64_t sz) {

    BufferStream stream(buf, sz);
    duckdb::BinaryDeserializer deserializer(stream);

    ReadVersion(deserializer);
    ASSERT(sz - stream.GetRemainder() == 8);

    while(!stream.Done()) {
        uint64_t log_record_size;
        uint64_t log_record_checksum;
        stream.ReadData((uint8_t*)&log_record_size, sizeof(log_record_size));
        stream.ReadData((uint8_t*)&log_record_checksum, sizeof(log_record_checksum));
        ASSERT(stream.GetRemainder() >= log_record_size);

        // parameter should be const here probably.
        // if it does write into it parameter, it will crash because of PROT_READ.
        std::unique_ptr<uint8_t[]> log_record_buf(new uint8_t[log_record_size]);
        stream.ReadData(log_record_buf.get(), log_record_size);
        uint64_t checksum = duckdb::Checksum(log_record_buf.get(), log_record_size);
        ASSERT(checksum == log_record_checksum);

        ParseWalRecord(std::move(log_record_buf), log_record_size, stream.GetOffset());
    }
    ASSERT(stream.GetRemainder() == 0)
}

int main(int argc, char** argv) {
    std::string wal_path = argv[1];
    auto wal_buffer = OpenWal(wal_path);
    ScanWal(wal_buffer.first.get(), wal_buffer.second);
    return 0;
}


