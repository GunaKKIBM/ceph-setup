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
#include <fstream>
#include <unordered_set>
#include <unordered_map>
#include <optional>
#define BLOCK_SIZE 4096 
#define BUFFER_SIZE BLOCK_SIZE 
#define FILENAME "filename.txt"
#define BLOCK_DEVICE "/var/lib/ceph/osd/ceph-0/block" 

struct IOEntry {
    size_t offset;
    std::chrono::steady_clock::time_point timestamp;
    uint32_t crc;
};

// Thread-safe queue
std::queue<IOEntry> io_queue;
std::mutex queue_mutex;
std::condition_variable queue_cv;
bool stop_threads = false; 

size_t total_file_size;

void prepare_iov(std::vector<iovec> &iov, void *buffer, size_t size) {
    iov.resize(1);
    iov[0].iov_base = buffer;
    //std::cout << "Printing buffer" << 
    iov[0].iov_len = BLOCK_SIZE;
}
// Function to generate a random offset (aligned to 4KB)
size_t generate_random_offset() {
    size_t max_blocks = total_file_size / BLOCK_SIZE;
    return (rand() % max_blocks) * BLOCK_SIZE;
}
uint32_t calculate_crc32(const char* buffer, size_t size) {
    return crc32(0, reinterpret_cast<const unsigned char*>(buffer), size);
}

bool exists_in_unsorted(const size_t* arr, size_t size, long long target) {
    std::unordered_set<size_t> elements(arr, arr + size);
    return elements.find(static_cast<size_t>(target)) != elements.end();
}

std::optional<size_t> find_index_in_unsorted(const size_t* arr, size_t size, long long target) {
    std::unordered_map<size_t, size_t> index_map;  // Maps value to its index

    for (size_t i = 0; i < size; ++i) {
        index_map[arr[i]] = i;  // Store the first occurrence
    }

    auto it = index_map.find(static_cast<size_t>(target));
    if (it != index_map.end()) {
        return it->second;  // Return the index if found
    }
    
    return std::nullopt;  // Return empty if not found
}

void remove_element(size_t arr[], size_t& size, int index) {
    if (index >= size) return;

    for (size_t i = index; i < size - 1; i++) {
        arr[i] = arr[i + 1]; // Shift elements left
    }
    size--; // Reduce the size
}

void remove_elementio(IOEntry arr[], size_t& size, int index) {
    if (index >= size) return;

    for (size_t i = index; i < size - 1; i++) {
        arr[i] = arr[i + 1]; // Shift elements left
    }
    size--; // Reduce the size
}


// Writer Thread
void writer_thread(int fd, int num_writes) {
    io_context_t ctx = 0; // AIO context
    int ret;

    /*
    //int fd = open(disk_path.c_str(), O_DIRECT | O_WRONLY | O_CREAT, 0644);
    int fd = open(BLOCK_DEVICE, O_RDWR | O_DIRECT);
    if (fd < 0) {
        perror("Error opening file for writing");
        return;
    }*/

     // Initialize the AIO context
    ret = io_setup(128, &ctx);
    if (ret < 0) {
        perror("io_setup failed");
        //close(fd);
        return;
    }

     // Allocate aligned memory for direct I/O
    char *write_buf;
    if (posix_memalign((void **)&write_buf, BLOCK_SIZE, BUFFER_SIZE)) {
        perror("posix_memalign failed");
        io_destroy(ctx);
        //close(fd);
        return;
    }

    // Fill write buffer with test data
    memset(write_buf, 'A', BUFFER_SIZE);
    uint32_t crc = calculate_crc32(write_buf, BLOCK_SIZE);


    struct iocb* cbs[num_writes];
    IOEntry crcoffsets[num_writes];
    size_t offsets[num_writes];
    for (int i = 0; i < num_writes; i++) {
        size_t offset = generate_random_offset();
        cbs[i] = new struct iocb(); 
	    struct iovec iov[1];
	    iov[0].iov_base = write_buf;
        iov[0].iov_len = BUFFER_SIZE;
        cbs[i]->aio_fildes = fd;
        crcoffsets[i].offset = offset;
        crcoffsets[i].crc = crc;
        offsets[i] = offset;
        io_prep_pwritev(cbs[i], fd, iov, 1, offset);
        //std::cout << "[Writer] at offset: " << offset << "CRC: " << crc << "\n" << std::endl;
        if ( offset == 0 ) {
            std::cout << "Gunaaaaaaa: " << offset << " CRC: " << crc << std::endl;
        }
        std::lock_guard<std::mutex> lock(queue_mutex);
        //io_queue.push({offset, std::chrono::steady_clock::now(), crc});
    }

    if (io_submit(ctx, num_writes, cbs) < 0) {
         perror("io_submit write failed");
    }

    struct io_event events[num_writes];
    //int num_events = io_getevents(ctx, 1, 1, events, NULL);
    /*if ( num_events < 0) {
        perror("io_getevents (write) failed");
    } else {
        printf("AIO write completed successfully\n");
    }*/

    size_t size = sizeof(offsets) / sizeof(offsets[0]);
    size_t size2 = sizeof(crcoffsets) / sizeof(crcoffsets[0]);
    size_t temp = static_cast<size_t>(num_writes); 
    int num_events;
    while (temp != 0) {
        num_events = io_getevents(ctx, 1, temp, events, NULL);
        if (num_events < 0) {
            perror("io_getevents (write) failed");
            return;
        }
        for (size_t j = 0; j < num_events; j++) {      
            auto result = find_index_in_unsorted(offsets, size, events[j].obj->u.c.offset);
        
            if (result) {
                std::cout << "Received event for "<< events[j].obj->u.c.offset << std::endl;
                io_queue.push({offsets[*result], std::chrono::steady_clock::now(), crcoffsets[*result].crc});
                remove_element(offsets, size, *result);
                remove_elementio(crcoffsets, size2, *result);
                temp = temp - 1;
            }
        }
    }
    queue_cv.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // Simulate IO delay

    io_destroy(ctx);
    //close(fd);
}

void reader_thread(int fd) {

    io_context_t ctx = 0; // AIO context
    int ret;

/*
    //int fd = open(disk_path.c_str(), O_DIRECT | O_WRONLY | O_CREAT, 0644);
    int fd = open(BLOCK_DEVICE, O_RDONLY | O_DIRECT);
    if (fd < 0) {
        perror("Error opening file for writing");
        return;
    }*/

     // Initialize the AIO context
    ret = io_setup(128, &ctx);
    if (ret < 0) {
        perror("io_setup failed");
        //close(fd);
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

        char *read_buf;
        if (posix_memalign((void **)&read_buf, BLOCK_SIZE, BUFFER_SIZE)) {
            perror("posix_memalign failed");
            io_destroy(ctx);
            //close(fd);
            return;
        }

        memset(read_buf, 0, BUFFER_SIZE);
        struct iocb cb;
        struct iocb* cbs[1];
        struct io_event events[1];
        struct iovec iov[1];
        iov[0].iov_base = read_buf;
        iov[0].iov_len = BUFFER_SIZE;
        cb.aio_fildes = fd;

        io_prep_preadv(&cb, fd, iov, 1, entry.offset);
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

        uint32_t read_crc = calculate_crc32(reinterpret_cast<const char*>(read_buf), BLOCK_SIZE);

        for (int i = 0; i < 4; i++) {
            if (read_crc == entry.crc) {
                 //std::cout << "[Reader] Read from offset: " << entry.offset
                 // << " Expected CRC: " << entry.crc
                  //<< " Read CRC: " << read_crc
                  //<< "CRC MATCH" << std::endl;
                  //io_destroy(ctx);
                  //close(fd);
                  //std::cout << "CRC matched" << std::endl;
                  return;
            }
            std::cout << "Retrying read: " << entry.offset << " Retry number: " << i << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
            if (io_submit(ctx, 1, cbs) < 0) {
                perror("io_submit read failed");
                continue;
            }
            if (io_getevents(ctx, 1, 1, events, nullptr) < 0) {
                perror("io_getevents failed");
                continue;
            }
            read_crc = calculate_crc32(reinterpret_cast<const char*>(read_buf), BLOCK_SIZE);
        }
		std::ofstream out("output.txt", std::ios::app);
		out << entry.offset <<  " Expected CRC: " << entry.crc << " Read CRC: " << read_crc << "\n";

        std::cout << "[Reader] Read from offset: " << entry.offset
                << " Expected CRC: " << entry.crc
                << " Read CRC: " << read_crc
                  <<  "CRC MISMATCH"
                  << std::endl;
        printf("%s\n", read_buf);
        io_destroy(ctx);
                  //close(fd);
                  return;
	}
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

    
    int fdwr = open(BLOCK_DEVICE, O_RDWR | O_DIRECT);
    if (fdwr < 0) {
        perror("Error opening file for writing");
        return 1;
    }

    /*
    int fdr = open(BLOCK_DEVICE, O_RDONLY | O_DIRECT);
    if (fdr < 0) {
        perror("Error opening file for writing");
        return 1;
    }*/

   //for (int j = 0; j < 20 ; j++) {

    for (int i = 0; i < num_writer_threads; i++) {
        writers.emplace_back(writer_thread, fdwr, 5);
    }

    for (int i = 0; i < num_reader_threads; i++) {
        readers.emplace_back(reader_thread, fdwr);
    }

    for (auto& t : writers) {
        t.join();
    }

    // Signal reader threads to stop
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        stop_threads = true;
    }
    queue_cv.notify_all();

    // Join reader threads
    for (auto& t : readers) {
        t.join();
    }
   //}
    close(fdwr);

    return 0;
}
