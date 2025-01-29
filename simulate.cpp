#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <random>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <libaio.h>
#include <zlib.h>

#define BLOCK_SIZE 4096 

struct IOEntry {
    size_t offset;
    std::chrono::steady_clock::time_point timestamp;
    uint32_t crc;
};


std::queue<IOEntry> io_queue;
std::mutex queue_mutex;
std::condition_variable queue_cv;
bool stop_threads = false; 

size_t total_file_size;

void prepare_iov(std::vector<iovec> &iov, void *buffer, size_t size) {
    iov.resize(1);
    iov[0].iov_base = buffer;
    iov[0].iov_len = 5
}
// generate a random offset (aligned to 4KB)
size_t generate_random_offset() {
    size_t max_blocks = total_file_size / BLOCK_SIZE;
    return (rand() % max_blocks) * BLOCK_SIZE;
}
uint32_t calculate_crc32(const char* buffer, size_t size) {
    return crc32(0, reinterpret_cast<const unsigned char*>(buffer), size);
}
/*
uint32_t calculate_crc(const std::vector<uint8_t>& data) {
    return crc32(0L, data.data(), data.size());
}*/

void writer_thread(const std::string& disk_path, int num_writes) {
    void *write_buffer;
    posix_memalign(&write_buffer, BLOCK_SIZE, BLOCK_SIZE);

    memset(write_buffer, 'A', BLOCK_SIZE);
    int fd = open(disk_path.c_str(), O_DIRECT | O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        perror("Error opening file for writing");
        return;
    }

    io_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(10, &ctx) < 0) {
        perror("io_setup failed");
        close(fd);
        return;
    }

   
    struct iocb* cbs[num_writes];

    for (int i = 0; i < num_writes; i++) {
        size_t offset = generate_random_offset();
        //std::vector<uint8_t> data(BLOCK_SIZE, rand() % 256);
        uint32_t crc = calculate_crc32(reinterpret_cast<const char*>(write_buffer), BLOCK_SIZE);

         struct iocb cb;

    //    struct iovec iov;
    //iov.iov_base = ;
    //iov.iov_len = BLOCK_SIZE;
        std::vector<iovec> iov;
        prepare_iov(iov, write_buffer, BLOCK_SIZE);
        cb.aio_lio_opcode = IO_CMD_PWRITEV;
        cb.aio_fildes = fd;
        //cb.u.c.buf = write_buffer;
        cb.u.c.buf = reinterpret_cast<void*>(&iov[0]);
        //std::cout << std::hex << data.data() << std::endl;
        cb.u.c.offset = offset;
        cb.u.c.nbytes = BLOCK_SIZE;
    
        //cb.u.c.data = 0;
        cbs[i] = &cb;

        std::cout << "[Writer] at offset: " << offset << "CRC: " << crc << std::endl;

       
            std::lock_guard<std::mutex> lock(queue_mutex);
            io_queue.push({offset, std::chrono::steady_clock::now(), crc});
    }
    if (io_submit(ctx, num_writes, cbs) < 0) {
         perror("io_submit write failed");
    }

        queue_cv.notify_all();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));  // simulate io delay

    io_destroy(ctx);
    close(fd);
}

void reader_thread(const std::string& disk_path) {
    int fd = open(disk_path.c_str(), O_DIRECT | O_RDONLY);
    if (fd < 0) {
        perror("Error opening file for reading");
        return;
    }

    io_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    if (io_setup(10, &ctx) < 0) {
        perror("io_setup failed");
        close(fd);
        return;
    }

    while (true) {
        IOEntry entry;

        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [] { return !io_queue.empty() || stop_threads; });

            if (stop_threads && io_queue.empty()) break;

            entry = io_queue.front();

            while (std::chrono::steady_clock::now() - entry.timestamp < std::chrono::seconds(2)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            io_queue.pop();
        }

        //std::vector<uint8_t> read_data(BLOCK_SIZE);
        void *read_buffer;
        posix_memalign(&read_buffer, 512, BLOCK_SIZE);
        
        struct iocb cb;
        struct iocb* cbs[1];
        struct io_event events[1];
        std::vector<iovec> iov;
        prepare_iov(iov, read_buffer, BLOCK_SIZE);

        cb.aio_lio_opcode = IO_CMD_PREADV;
        cb.aio_fildes = fd;
        cb.u.c.buf = reinterpret_cast<void*>(&iov[0]);
        cb.u.c.offset = entry.offset;
        cb.u.c.nbytes = BLOCK_SIZE;
        //cb.adata = 0;
        cbs[0] = &cb;

        if (io_submit(ctx, 1, cbs) < 0) {
            perror("io_submit read failed");
            continue;
        }

        // Wait for the read to complete
        if (io_getevents(ctx, 1, 1, events, nullptr) < 0) {
            perror("io_getevents failed");
            continue;
        }

        //std::cout << std::hex <<  read_data.data() << std::endl;
        uint32_t read_crc = calculate_crc32(reinterpret_cast<const char*>(read_buffer), BLOCK_SIZE);

        std::cout << "[Reader] Read from offset: " << entry.offset
                  << " expected CRC: " << entry.crc
                  << " read CRC: " << read_crc
                  << " | " << (read_crc == entry.crc ? "CRC MATCH" : "CRC MISMATCH")
                  << std::endl;
    }

    io_destroy(ctx);
    close(fd);
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <disk_file_path> <file_size> <num_writer_threads> <num_reader_threads>" << std::endl;
        return 1;
    }

    srand(time(0));

    std::string disk_path = argv[1];
    total_file_size = std::stoul(argv[2]);
    int num_writer_threads = std::stoi(argv[3]);
    int num_reader_threads = std::stoi(argv[4]);

    std::vector<std::thread> writers;
    std::vector<std::thread> readers;

    for (int i = 0; i < num_writer_threads; i++) {
        writers.emplace_back(writer_thread, disk_path, 5);
    }

    for (int i = 0; i < num_reader_threads; i++) {
        readers.emplace_back(reader_thread, disk_path);
    }

    for (auto& t : writers) {
        t.join();
    }

    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        stop_threads = true;
    }
    queue_cv.notify_all();

    for (auto& t : readers) {
        t.join();
    }

    return 0;
}
