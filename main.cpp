
#include <_types/_uint8_t.h>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <sys/stat.h>
#include "duckdb.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/serializer/serialization_traits.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include "duckdb/common/serializer/encoding_util.hpp"
#include <fcntl.h>
#include <type_traits>

#define ASSERT(ret) \
    if (!(ret)) {           \
        std::cerr << "failed assertion " << #ret << std::endl; \
        std::abort();        \
    }

using WalUnderlyingType = typename std::underlying_type<duckdb::WALType>::type;

template<typename Buffer>
struct WalBuffer {
    Buffer buf;
    uint64_t size;
};

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

    WalBuffer<decltype(b)> ret{std::move(b), 
        static_cast<uint64_t>(st.st_size)};

    return ret;
}

void ReadVersion(uint8_t** buf) {
    duckdb::field_id_t field;
    field = *(duckdb::field_id_t*)(*buf);
    // https://github.com/duckdb/duckdb/blob/v1.1.3/src/storage/write_ahead_log.cpp#L164
    ASSERT(field == 100);
    (*buf)+=sizeof(field);

    WalUnderlyingType wal_type;
    uint64_t num_read = duckdb::EncodingUtil::DecodeUnsignedLEB128(*buf, wal_type);
    ASSERT(wal_type == static_cast<WalUnderlyingType>(duckdb::WALType::WAL_VERSION));
    (*buf)+=num_read;

    field = *(duckdb::field_id_t*)*buf;
    ASSERT(field == 101);
    (*buf)+=sizeof(field);

    duckdb::idx_t wal_version_number;
    num_read = duckdb::EncodingUtil::DecodeUnsignedLEB128(*buf, wal_version_number);
    (*buf) += num_read;
    ASSERT(wal_version_number == 2);

    field = *(duckdb::field_id_t*)*buf;
    ASSERT(field == duckdb::MESSAGE_TERMINATOR_FIELD_ID);
    (*buf)+=sizeof(field);
}

void ScanWal(uint8_t* buf, uint64_t sz) {
    uint8_t* end = buf + sz;
    uint8_t* start = buf;
    ReadVersion(&buf);
    ASSERT(buf - start == 8);

    while(buf < end) {
        uint64_t log_record_size = *(uint64_t*)buf;
        buf += 8;
        uint64_t log_record_checksum = *(uint64_t*)buf;
        buf += 8;
        ASSERT((end - buf) >= log_record_size);

        // parameter should be const here probably.
        // if it does write into it parameter, it will crash because of PROT_READ.
        uint64_t checksum = duckdb::Checksum(buf, log_record_size);
        ASSERT(checksum == log_record_checksum);
        buf += log_record_size;
    }
    ASSERT(buf == end);
}

int main(int argc, char** argv) {
    std::string wal_path = argv[1];
    auto wal_buffer = OpenWal(wal_path);
    ScanWal(wal_buffer.buf.get(), wal_buffer.size);
    return 0;
}


