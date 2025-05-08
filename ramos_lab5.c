#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h> // Include for uint8_t
#include <math.h>   // Include for sqrt

#define MAX_SLAVES 16
#define BUFFER_SIZE (1 * 1024 * 1024)  // 1MB buffer
#define CONFIG_FILE "config.txt"
#define CHUNK_SIZE (state->n / (state->t * 10)) // Rows per chunk

typedef struct {
    char ip[16];
    int port;
} SlaveInfo;

typedef struct {
    int **matrix; // Change matrix type to int
    int n;        // Matrix size
    int p;        // Port number
    int s;        // Status (0=master, 1=slave)
    int t;        // Number of slaves
    SlaveInfo slaves[MAX_SLAVES];
} ProgramState;

typedef struct {
    ProgramState *state;
    int slave_index;
    int start_row;
    int rows_for_this_slave;
} ThreadArgs;

void read_config(ProgramState *state, int required_slaves) {
    FILE *file = fopen(CONFIG_FILE, "r");
    if (!file) {
        perror("Failed to open config file");
        exit(EXIT_FAILURE);
    }

    state->t = 0;
    char line[100];
    while (state->t < required_slaves && fgets(line, sizeof(line), file)) {
        sscanf(line, "%s %d", state->slaves[state->t].ip, &state->slaves[state->t].port);
        state->t++;
    }
    fclose(file);
}

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

void free_matrix(ProgramState *state) {
    if (state->matrix) {
        for (int i = 0; i < state->n; i++) {
            free(state->matrix[i]);
        }
        free(state->matrix);
    }
}

void create_matrix(ProgramState *state) {
    srand(time(NULL));
    for (int i = 0; i < state->n; i++) {
        for (int j = 0; j < state->n; j++) {
            state->matrix[i][j] = rand() % 100 + 1;
        }
    }
}

#include <unistd.h> // For usleep
#include <sys/time.h> // For timing

void *send_to_slave(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    ProgramState *state = args->state;
    int slave = args->slave_index;
    int start_row = args->start_row;
    int rows_for_this_slave = args->rows_for_this_slave;

    printf("Sending data to slave %d at IP %s, Port %d\n", 
           slave, state->slaves[slave].ip, state->slaves[slave].port);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Set TCP_NODELAY and buffer sizes
    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
    int buf_size = BUFFER_SIZE;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

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

    // Start timing
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL);

    // Send data in chunks
    printf("Sending rows %d to %d to slave %d\n", 
           start_row, start_row + rows_for_this_slave - 1, slave);

    size_t total_bytes_sent = 0; // Track total bytes sent

    for (int i = 0; i < rows_for_this_slave; i += CHUNK_SIZE) {
        int rows_to_send = (i + CHUNK_SIZE > rows_for_this_slave) ? 
                          (rows_for_this_slave - i) : CHUNK_SIZE;
        int total_bytes = rows_to_send * state->n * sizeof(int);
        total_bytes_sent += total_bytes;

        // Allocate temporary buffer
        int *buffer = malloc(total_bytes);
        for (int j = 0; j < rows_to_send; j++) {
            memcpy(buffer + j * state->n, state->matrix[start_row + i + j], 
                   state->n * sizeof(int));
        }
        
        // Send the chunk
        if (send(sock, buffer, total_bytes, 0) != total_bytes) {
            perror("Failed to send matrix chunk");
            free(buffer);
            exit(EXIT_FAILURE);
        }
        free(buffer);

        usleep(1000); // 1 millisecond delay
    }

    // Add logic to send vector y in send_to_slave
    if (send(sock, state->matrix[start_row], rows_for_this_slave * state->n * sizeof(int), 0) !=
        rows_for_this_slave * state->n * sizeof(int)) {
        perror("Failed to send matrix chunk");
        exit(EXIT_FAILURE);
    }

    // Send vector y
    if (send(sock, state->matrix[0], state->n * sizeof(int), 0) != state->n * sizeof(int)) {
        perror("Failed to send vector y");
        exit(EXIT_FAILURE);
    }

    // End timing
    gettimeofday(&time_after, NULL);

    // Calculate elapsed time
    double elapsed = (time_after.tv_sec - time_before.tv_sec) + 
                     (time_after.tv_usec - time_before.tv_usec) / 1000000.0;

    // Calculate Mbps
    double mbps = (total_bytes_sent * 8) / (elapsed * 1000000.0); // Convert bytes to bits, then to Mbps
    printf("Slave %d: Sent %zu bytes in %.6f seconds (%.2f Mbps)\n", 
           slave, total_bytes_sent, elapsed, mbps);

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

    close(sock);
    return NULL;
}

void distribute_submatrices(ProgramState *state) {
    struct timeval time_before, time_after;
    gettimeofday(&time_before, NULL);

    int slave_count = state->t;
    int base_rows_per_slave = state->n / slave_count;
    int extra_rows = state->n % slave_count;
    int start_row = 0;

    pthread_t threads[MAX_SLAVES];
    ThreadArgs args[MAX_SLAVES];

    double *mse_results = (double *)malloc(state->n * sizeof(double));
    if (!mse_results) {
        perror("MSE results allocation failed");
        exit(EXIT_FAILURE);
    }

    for (int slave = 0; slave < slave_count; slave++) {
        args[slave].state = state;
        args[slave].slave_index = slave;
        args[slave].start_row = start_row;
        args[slave].rows_for_this_slave = base_rows_per_slave + (slave < extra_rows ? 1 : 0);

        pthread_create(&threads[slave], NULL, send_to_slave, &args[slave]);

        start_row += args[slave].rows_for_this_slave;
    }

    // Wait for all threads to complete
    for (int slave = 0; slave < slave_count; slave++) {
        pthread_join(threads[slave], NULL);
    }

    // Collect MSE results from slaves
    for (int slave = 0; slave < slave_count; slave++) {
        // Assume MSE results are collected here
        // This part depends on how the results are sent back from the slaves
    }

    gettimeofday(&time_after, NULL);
    double elapsed = (time_after.tv_sec - time_before.tv_sec) +
                     (time_after.tv_usec - time_before.tv_usec) / 1000000.0;
    printf("Master elapsed time: %.6f seconds\n", elapsed);

    free(mse_results);
}

void print_matrix(int **matrix, int rows, int cols) {
    printf("Received matrix:\n");
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            printf("%d ", matrix[i][j]);
        }
        printf("\n");
    }
}

void slave_listen(ProgramState *state) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Set socket options
    int flag = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
    int buf_size = BUFFER_SIZE;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

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

    struct timeval time_before, time_after;

    int addrlen = sizeof(address);
    int master_sock = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
    if (master_sock < 0) {
        perror("Accept failed");
        exit(EXIT_FAILURE);
    }

    gettimeofday(&time_before, NULL);

    // Receive submatrix size info
    int info[2];
    if (recv(master_sock, info, sizeof(info), 0) != sizeof(info)) {
        perror("Failed to receive matrix info");
        exit(EXIT_FAILURE);
    }
    int rows = info[0];
    int cols = info[1];

    printf("Slave received matrix size: %d rows x %d cols\n", rows, cols);

    // Allocate memory for submatrix and vector y
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

    int *vector_y = (int *)malloc(cols * sizeof(int));
    if (!vector_y) {
        perror("Vector y allocation failed");
        exit(EXIT_FAILURE);
    }

    // Receive the submatrix data
    for (int i = 0; i < rows; i++) {
        if (recv(master_sock, submatrix[i], cols * sizeof(int), 0) != cols * sizeof(int)) {
            perror("Failed to receive matrix row");
            exit(EXIT_FAILURE);
        }
    }

    // Receive the vector y
    if (recv(master_sock, vector_y, cols * sizeof(int), 0) != cols * sizeof(int)) {
        perror("Failed to receive vector y");
        exit(EXIT_FAILURE);
    }

    printf("Slave received submatrix and vector y.\n");

    // Compute MSE
    double *mse_results = (double *)malloc(rows * sizeof(double));
    if (!mse_results) {
        perror("MSE results allocation failed");
        exit(EXIT_FAILURE);
    }

    gettimeofday(&time_before, NULL);
    for (int i = 0; i < rows; i++) {
        double mse = 0.0;
        for (int j = 0; j < cols; j++) {
            double diff = submatrix[i][j] - vector_y[j];
            mse += diff * diff;
        }
        mse_results[i] = sqrt(mse / cols);
    }
    gettimeofday(&time_after, NULL);

    double elapsed = (time_after.tv_sec - time_before.tv_sec) +
                     (time_after.tv_usec - time_before.tv_usec) / 1000000.0;
    printf("Slave computed MSE in %.6f seconds.\n", elapsed);

    // Send MSE results back to master
    if (send(master_sock, mse_results, rows * sizeof(double), 0) != rows * sizeof(double)) {
        perror("Failed to send MSE results");
        exit(EXIT_FAILURE);
    }

    printf("Slave sent MSE results to master.\n");

    // Free allocated memory
    for (int i = 0; i < rows; i++) {
        free(submatrix[i]);
    }
    free(submatrix);
    free(vector_y);
    free(mse_results);

    close(master_sock);
    close(server_fd);
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

        read_config(&state, state.t);
        allocate_matrix(&state);
        create_matrix(&state);

        //Optional: print matrix
        //print_matrix(state.matrix, state.n, state.n);

        distribute_submatrices(&state);
        free_matrix(&state);
    } else {
        slave_listen(&state);
    }

    return EXIT_SUCCESS;
}
