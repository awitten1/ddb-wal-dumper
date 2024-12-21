
#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sys/mman.h>
#include <sys/stat.h>
#include "duckdb.hpp"
#include "duckdb/storage/write_ahead_log.hpp"
#include <fcntl.h>

#define ASSERT(ret) \
    if (!(ret)) {           \
        std::cerr << "failed assertion " << #ret << std::endl; \
        std::abort();        \
    }

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
    char* buf = (char*)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    ASSERT(buf != MAP_FAILED);
    auto rel = [st](char* buf) {
        munmap(buf, st.st_size);
    };

    std::unique_ptr<char, decltype(rel)> b(buf, rel);

    madvise(buf, st.st_size, MADV_SEQUENTIAL);
    WalBuffer<decltype(b)> ret{std::move(b), 
        static_cast<uint64_t>(st.st_size)};
    return ret;
}

void ScanWal(char* buf, uint64_t sz) {
    char* end = buf + sz;
    while(buf < end) {
        uint64_t log_record_size = *(uint64_t*)buf;
        buf += 8;
        uint64_t log_record_checksum = *(uint64_t*)buf;
        buf += 8;
        //ASSERT((end - buf) > log_record_size);
        std::cout << (end - buf) << std::endl;
        std::cout << log_record_size << std::endl;
        buf += log_record_size;
    }
}

int main(int argc, char** argv) {
    std::string wal_path = argv[1];
    auto wal_buffer = OpenWal(wal_path);
    ScanWal(wal_buffer.buf.get(), wal_buffer.size);
    return 0;
}


