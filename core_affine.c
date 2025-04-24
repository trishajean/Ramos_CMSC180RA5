#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <sched.h> // For sched_setaffinity
#include <pthread.h>

#define MAX_SLAVES 16
#define BUFFER_SIZE 1024
#define CONFIG_FILE "config.txt"

typedef struct {
    char ip[16];
    int port;
} SlaveInfo;

typedef struct {
    int n;       // Matrix size
    int p;       // Port number
    int s;       // Status (0=master, 1=slave)
    int t;       // Number of slaves
    SlaveInfo slaves[MAX_SLAVES];
    int **matrix;
} ProgramState;

typedef struct {
    int start_col;
    int end_col;
    int **submatrix;
    int *vector_y;
    double *mse;
    int rows;
} ThreadData;

// Function to read slave configuration from config.txt
void read_config(ProgramState *state, int required_slaves) {
    FILE *file = fopen(CONFIG_FILE, "r");
    if (!file) {
        perror("Failed to open config file");
        exit(EXIT_FAILURE);
    }

    state->t = 0;
    char line[100];
    while (fgets(line, sizeof(line), file) && state->t < required_slaves) {
        sscanf(line, "%s %d", state->slaves[state->t].ip, &state->slaves[state->t].port);
        state->t++;
    }
    fclose(file);
}

// Allocate memory for the matrix
void allocate_matrix(ProgramState *state) {
    state->matrix = (int **)malloc(state->n * sizeof(int *));
    if (!state->matrix) {
        perror("Matrix allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < state->n; i++) {
        state->matrix[i] = (int *)malloc(state->n * sizeof(int));
        if (!state->matrix[i]) {
            perror("Matrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }
}

// Free the allocated memory for the matrix
void free_matrix(ProgramState *state) {
    if (state->matrix) {
        for (int i = 0; i < state->n; i++) {
            free(state->matrix[i]);
        }
        free(state->matrix);
    }
}

// Initialize the matrix with random values
void create_matrix(ProgramState *state) {
    srand(time(NULL));
    for (int i = 0; i < state->n; i++) {
        for (int j = 0; j < state->n; j++) {
            state->matrix[i][j] = rand() % 100 + 1; // Random number between 1 and 100
        }
    }
}

// Write matrix to a file
void write_matrix_to_file(ProgramState *state, const char *filename) {
    FILE *file = fopen(filename, "wb");
    if (!file) {
        perror("Failed to open file for writing");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < state->n; i++) {
        fwrite(state->matrix[i], sizeof(int), state->n, file);
    }

    fclose(file);
}

// Load specific rows from the file
void load_rows_from_file(const char *filename, int start_row, int end_row, int **rows, int n) {
    FILE *file = fopen(filename, "rb");
    if (!file) {
        perror("Failed to open file for reading");
        exit(EXIT_FAILURE);
    }

    for (int i = start_row; i <= end_row; i++) {
        fseek(file, i * n * sizeof(int), SEEK_SET);
        fread(rows[i - start_row], sizeof(int), n, file);
    }

    fclose(file);
}

// Distribute submatrices to slaves
void distribute_submatrices(ProgramState *state, int *vector_y, int size) {
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL);

    int slave_count = state->t;  // Get slave count from state
    int base_rows_per_slave = state->n / slave_count;
    int extra_rows = state->n % slave_count;  // Remaining rows to distribute

    int start_row = 0;

    for (int slave = 0; slave < slave_count; slave++) {
        int rows_for_this_slave = base_rows_per_slave + (slave < extra_rows ? 1 : 0);  // Add 1 row for the first 'extra_rows' slaves
        printf("Sending data to slave %d at IP %s, Port %d\n", slave, state->slaves[slave].ip, state->slaves[slave].port);

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        int buffer_size = 1024 * 1024; // 1 MB
        setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

        struct sockaddr_in slave_addr;
        memset(&slave_addr, 0, sizeof(slave_addr));
        slave_addr.sin_family = AF_INET;
        slave_addr.sin_port = htons(state->slaves[slave].port);
        inet_pton(AF_INET, state->slaves[slave].ip, &slave_addr.sin_addr);

        if (connect(sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
            perror("Connection failed");
            exit(EXIT_FAILURE);
        }

        // Send submatrix size info
        int info[2] = {rows_for_this_slave, state->n};
        if (send(sock, info, sizeof(info), 0) != sizeof(info)) {
            perror("Failed to send matrix info");
            exit(EXIT_FAILURE);
        }

        // Send the submatrix data in chunks
        printf("Sending rows %d to %d to slave %d\n", start_row, start_row + rows_for_this_slave - 1, slave);
        for (int i = 0; i < rows_for_this_slave; i++) {
            int *row = state->matrix[start_row + i];

            ssize_t bytes_sent = 0;
            ssize_t total_bytes = state->n * sizeof(int);
            while (bytes_sent < total_bytes) {
                ssize_t result = send(sock, (char *)row + bytes_sent, total_bytes - bytes_sent, 0);
                if (result < 0) {
                    perror("Failed to send matrix row");
                    exit(EXIT_FAILURE);
                }
                bytes_sent += result;
            }
        }

        // Wait for acknowledgment
        char ack[4];
        if (recv(sock, ack, sizeof(ack), 0) != sizeof(ack)) {
            perror("Failed to receive acknowledgment");
            exit(EXIT_FAILURE);
        }

        if (strcmp(ack, "ack") != 0) {
            fprintf(stderr, "Did not receive proper acknowledgment from slave %d\n", slave);
            exit(EXIT_FAILURE);
        }

        // Send vector y
        printf("Sending vector y to slave %d\n", slave);
        ssize_t bytes_sent = 0;
        ssize_t total_bytes = size * sizeof(int);
        while (bytes_sent < total_bytes) {
            ssize_t result = send(sock, (char *)vector_y + bytes_sent, total_bytes - bytes_sent, 0);
            if (result < 0) {
                perror("Failed to send vector y");
                exit(EXIT_FAILURE);
            }
            bytes_sent += result;
        }

        printf("Successfully sent vector y to slave %d\n", slave);

        close(sock);

        // Update start_row for the next slave
        start_row += rows_for_this_slave;
    }

    gettimeofday(&time_after, NULL);
    double elapsed = (time_after.tv_sec - time_before.tv_sec) + 
                    (time_after.tv_usec - time_before.tv_usec) / 1000000.0;
    printf("Master elapsed time: %.6f seconds\n", elapsed);
}

// Broadcast vector_y to all slaves
void broadcast_vector_y(ProgramState *state, int *vector_y, int size) {
    struct timeval start, end;
    gettimeofday(&start, NULL);

    int chunk_size = 1024; // Send in chunks of 1024 elements
    for (int slave = 0; slave < state->t; slave++) {
        printf("Sending vector y to slave %d at IP %s, Port %d\n", slave, state->slaves[slave].ip, state->slaves[slave].port);

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        int buffer_size = 1024 * 1024; // 1 MB
        setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

        struct timeval timeout;
        timeout.tv_sec = 120; // Increase to 120 seconds
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        struct sockaddr_in slave_addr;
        memset(&slave_addr, 0, sizeof(slave_addr));
        slave_addr.sin_family = AF_INET;
        slave_addr.sin_port = htons(state->slaves[slave].port);
        inet_pton(AF_INET, state->slaves[slave].ip, &slave_addr.sin_addr);

        if (connect(sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
            perror("Connection to slave failed");
            close(sock);
            exit(EXIT_FAILURE);
        }

        // Send vector size
        if (send(sock, &size, sizeof(size), 0) != sizeof(size)) {
            perror("Failed to send vector size");
            close(sock);
            exit(EXIT_FAILURE);
        }

        // Send vector data in chunks
        for (int i = 0; i < size; i += chunk_size) {
            int current_chunk_size = (i + chunk_size > size) ? size - i : chunk_size;
            if (send(sock, vector_y + i, current_chunk_size * sizeof(int), 0) != current_chunk_size * sizeof(int)) {
                perror("Failed to send vector chunk");
                close(sock);
                exit(EXIT_FAILURE);
            }
        }

        printf("Successfully sent vector y to slave %d\n", slave);
        close(sock);
    }

    gettimeofday(&end, NULL);
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    printf("Elapsed time: %.6f seconds\n", elapsed);
}

// Set CPU affinity for the current process
void set_core_affinity(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("Failed to set CPU affinity");
    } else {
        printf("Process bound to core %d\n", core_id);
    }
}

void *compute_mse_thread(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    for (int j = data->start_col; j < data->end_col; j++) {
        double sum = 0.0;
        for (int i = 0; i < data->rows; i++) {
            double diff = data->submatrix[i][j] - data->vector_y[j];
            sum += diff * diff;
        }
        data->mse[j] = sum / data->rows;
    }
    return NULL;
}

void compute_mse_parallel(int **submatrix, int rows, int cols, int *vector_y, double *mse) {
    int num_threads = 4; // Adjust based on the number of CPU cores
    pthread_t threads[num_threads];
    ThreadData thread_data[num_threads];

    int cols_per_thread = cols / num_threads;
    for (int t = 0; t < num_threads; t++) {
        thread_data[t].start_col = t * cols_per_thread;
        thread_data[t].end_col = (t == num_threads - 1) ? cols : (t + 1) * cols_per_thread;
        thread_data[t].submatrix = submatrix;
        thread_data[t].vector_y = vector_y;
        thread_data[t].mse = mse;
        thread_data[t].rows = rows;

        pthread_create(&threads[t], NULL, compute_mse_thread, &thread_data[t]);
    }

    for (int t = 0; t < num_threads; t++) {
        pthread_join(threads[t], NULL);
    }
}

// Function to compute MSE only
double *compute_mse_only(int **submatrix, int rows, int cols, int *vector_y) {
    double *mse = (double *)malloc(cols * sizeof(double));
    if (!mse) {
        perror("MSE allocation failed");
        exit(EXIT_FAILURE);
    }

    compute_mse_parallel(submatrix, rows, cols, vector_y, mse);
    return mse;
}

// Function to send MSE to master
void send_mse_to_master(ProgramState *state, double *mse, int cols) {
    printf("Slave connecting to master at IP %s, Port %d to send MSEs...\n", state->slaves[0].ip, state->p);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    int buffer_size = 1024 * 1024; // 1 MB
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

    struct sockaddr_in master_addr;
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(state->p);
    inet_pton(AF_INET, state->slaves[0].ip, &master_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0) {
        perror("Connection to master failed");
        exit(EXIT_FAILURE);
    }

    // Send MSE vector
    ssize_t total_bytes = cols * sizeof(double);
    if (send(sock, mse, total_bytes, 0) != total_bytes) {
        perror("Failed to send MSEs");
        exit(EXIT_FAILURE);
    }

    printf("Slave successfully sent MSEs to master\n");
    close(sock);
}

void slave_listen(ProgramState *state) {
    printf("Slave on port %d starting...\n", state->p);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    int buffer_size = 1024 * 1024; // 1 MB
    setsockopt(server_fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
    setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(state->p);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 3) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Slave listening on port %d...\n", state->p);

    int addrlen = sizeof(address);
    int master_sock = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
    if (master_sock < 0) {
        perror("Accept failed");
        exit(EXIT_FAILURE);
    }

    // Receive submatrix size info
    int info[2];
    if (recv(master_sock, info, sizeof(info), 0) != sizeof(info)) {
        perror("Failed to receive matrix info");
        exit(EXIT_FAILURE);
    }
    int rows = info[0];
    int cols = info[1];

    // Allocate memory for submatrix
    int **submatrix = (int **)malloc(rows * sizeof(int *));
    if (!submatrix) {
        perror("Submatrix allocation failed");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < rows; i++) {
        submatrix[i] = (int *)malloc(cols * sizeof(int));
        if (!submatrix[i]) {
            perror("Submatrix row allocation failed");
            exit(EXIT_FAILURE);
        }
    }

    // Receive the submatrix data
    for (int i = 0; i < rows; i++) {
        ssize_t bytes_received = 0;
        ssize_t total_bytes = cols * sizeof(int);
        while (bytes_received < total_bytes) {
            ssize_t result = recv(master_sock, (char *)submatrix[i] + bytes_received, 
                                 total_bytes - bytes_received, 0);
            if (result < 0) {
                perror("Failed to receive matrix row");
                exit(EXIT_FAILURE);
            }
            bytes_received += result;
        }
    }

    // Send acknowledgment
    if (send(master_sock, "ack", 4, 0) != 4) {
        perror("Failed to send acknowledgment");
        exit(EXIT_FAILURE);
    }

    printf("Slave processed %d rows\n", rows);

    // Receive vector y
    int *vector_y = (int *)malloc(cols * sizeof(int));
    if (!vector_y) {
        perror("Vector y allocation failed");
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_received = 0;
    ssize_t total_bytes = cols * sizeof(int);
    while (bytes_received < total_bytes) {
        ssize_t result = recv(master_sock, (char *)vector_y + bytes_received, total_bytes - bytes_received, 0);
        if (result < 0) {
            perror("Failed to receive vector y");
            exit(EXIT_FAILURE);
        }
        bytes_received += result;
    }

    printf("Slave received vector y\n");

    // Compute MSE
    double *mse = compute_mse_only(submatrix, rows, cols, vector_y);

    // Send MSE to master
    send_mse_to_master(state, mse, cols);

    // Clean up
    for (int i = 0; i < rows; i++) {
        free(submatrix[i]);
    }
    free(submatrix);
    free(vector_y);
    free(mse);
    close(master_sock);
    close(server_fd);
}

// Receive MSEs from slaves
void receive_mse_from_slaves(ProgramState *state, double *mse_vector, int cols) {
    int offset = 0;

    for (int slave = 0; slave < state->t; slave++) {
        printf("Master listening for MSEs from slave %d on port %d...\n", slave, state->slaves[slave].port);

        int server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        int buffer_size = 1024 * 1024; // 1 MB
        setsockopt(server_fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
        setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

        struct sockaddr_in address;
        memset(&address, 0, sizeof(address));
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(state->slaves[slave].port);

        if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
            perror("Bind failed");
            exit(EXIT_FAILURE);
        }

        if (listen(server_fd, 1) < 0) {
            perror("Listen failed");
            exit(EXIT_FAILURE);
        }

        int addrlen = sizeof(address);
        int slave_sock = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);
        if (slave_sock < 0) {
            perror("Accept failed");
            exit(EXIT_FAILURE);
        }

        // Receive MSE vector
        ssize_t bytes_received = 0;
        ssize_t total_bytes = cols * sizeof(double);
        while (bytes_received < total_bytes) {
            ssize_t result = recv(slave_sock, (char *)mse_vector + offset + bytes_received, total_bytes - bytes_received, 0);
            if (result < 0) {
                perror("Failed to receive MSEs");
                exit(EXIT_FAILURE);
            }
            bytes_received += result;
        }

        printf("Successfully received MSEs from slave %d\n", slave);
        offset += cols;

        close(slave_sock);
        close(server_fd);
    }
}

int main() {
    ProgramState state;

    printf("Input matrix size (n): ");
    scanf("%d", &state.n);

    if (state.n <= 0) {
        printf("Invalid matrix size. Must be positive\n");
        return EXIT_FAILURE;
    }

    printf("Input port number: ");
    scanf("%d", &state.p);

    printf("Input status (0 = master, 1 = slave): ");
    scanf("%d", &state.s);

    state.matrix = NULL;
    state.t = 0;

    if (state.s == 0) {
        printf("Input number of slaves: ");
        scanf("%d", &state.t);

        if (state.t <= 0 || state.t > MAX_SLAVES) {
            printf("Invalid number of slaves. Must be between 1 and %d\n", MAX_SLAVES);
            return EXIT_FAILURE;
        }

        printf("Running as master with %d slaves\n", state.t);

        // Read slave configuration from config.txt
        read_config(&state, state.t);

        // Master logic for matrix allocation and distribution
        allocate_matrix(&state);
        create_matrix(&state);

        int *vector_y = (int *)malloc(state.n * sizeof(int));
        if (!vector_y) {
            perror("Vector y allocation failed");
            exit(EXIT_FAILURE);
        }

        // Initialize vector y with random values
        for (int i = 0; i < state.n; i++) {
            vector_y[i] = rand() % 100 + 1;
        }

        distribute_submatrices(&state, vector_y, state.n);
        sleep(1); // Allow slaves to prepare for receiving vector y

        printf("Broadcasting vector y:\n");
        broadcast_vector_y(&state, vector_y, state.n);

        // Collect MSEs from slaves
        double *mse_vector = (double *)malloc(state.n * sizeof(double));
        if (!mse_vector) {
            perror("MSE vector allocation failed");
            exit(EXIT_FAILURE);
        }

        receive_mse_from_slaves(&state, mse_vector, state.n);

        free(vector_y);
        free(mse_vector);
        free_matrix(&state);
    } else {
        // Slave logic
        slave_listen(&state);
    }

    return EXIT_SUCCESS;
}

