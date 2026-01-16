// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>
#include <sys/ioctl.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;
static int efd;
static const char fmt_200[] = "HTTP/1.1 200 OK\r\n"
								"Content-Length: ";
static const char fmt_400[] = "HTTP/1.1 404 Not Found\r\n"
					"Content-Length: 0\r\n"
					"Connection: close\r\n"
					"\r\n";

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memmove(conn->request_path, buf, len);
	memset(conn->request_path + len, 0, 1);
	conn->have_path = 1;

	return 0;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	const char *s = "/static/";
	const char *d = "/dynamic/";

	if (!memcmp(conn->request_path, s, strlen(s)))
		return RESOURCE_TYPE_STATIC;
	if (!memcmp(conn->request_path, d, strlen(d)))
		return RESOURCE_TYPE_DYNAMIC;

	return RESOURCE_TYPE_NONE;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	char *p = conn->send_buffer;

	p = stpcpy(p, fmt_200);
	p += sprintf(p, "%ld", (long)conn->file_size);
	p = stpcpy(p, "\r\nConnection: close\r\n\r\n");

	conn->send_len = p - conn->send_buffer;
	conn->send_pos = 0;
	conn->state = STATE_SENDING_HEADER;
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	memmove(conn->send_buffer, fmt_400, strlen(fmt_400));
	conn->send_len = strlen(fmt_400);
	conn->send_pos = 0;
	conn->state = STATE_SENDING_404;
	conn->file_size = 0;
	conn->file_pos = 0;
	if (conn->fd >= 0) {
		close(conn->fd);
		conn->fd = -1;
	}
}

struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn = calloc(1, sizeof(*conn));

	DIE(conn == NULL, "calloc");

	conn->sockfd = sockfd;
	conn->fd = -1;
	conn->state = STATE_INITIAL;
	conn->res_type = RESOURCE_TYPE_NONE;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	if (connection_get_resource_type(conn) != RESOURCE_TYPE_DYNAMIC)
		return;

	conn->header_len = snprintf(conn->send_buffer, BUFSIZ,
		"%s%ld\r\n\r\n", fmt_200, conn->file_size);

	char *dst = conn->send_buffer + conn->header_len;
	char *end = dst + conn->file_size;

	ssize_t r;

	for (char *p = dst; p < end; p += r) {
		r = read(conn->fd, p, end - p);

		if (r <= 0)
			break;
	}

	ssize_t body_len = end - (conn->send_buffer + conn->header_len);

	conn->send_len = conn->header_len + body_len;
	conn->send_pos = 0;
	conn->state = STATE_SENDING_DATA;

	memset(&conn->iocb, 0, sizeof(struct iocb));
	conn->piocb[0] = &conn->iocb;

	io_prep_pread(conn->piocb[0], conn->fd, conn->send_buffer, 0, 0);
	io_submit(ctx, 1, conn->piocb);

	close(conn->fd);
	conn->fd = -1;
	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	if (conn->fd >= 0) {
		close(conn->fd);
		conn->fd = -1;
	}
	close(conn->sockfd);
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc, one = 1;

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	/* TODO: Set socket to be non-blocking. */
	rc = ioctl(sockfd, FIONBIO, &one); DIE(rc < 0, "ioctl FIONBIO");

	/* TODO: Instantiate new connection handler. */
	conn = connection_create(sockfd);

	/* TODO: Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_in");

	/* TODO: Initialize HTTP_REQUEST parser. */
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	char *pos = conn->recv_buffer + conn->recv_len;
	char *limit = conn->recv_buffer + BUFSIZ;

	for (ssize_t r = 0; pos < limit; pos += r) {
		r = recv(conn->sockfd, pos, limit - pos, 0);

		if (r < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
			connection_remove(conn);
			return;
		} else if (r == 0) {
			if (pos == conn->recv_buffer) {
				w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
				connection_remove(conn);
				return;
			}
			break;
		}
		conn->recv_len += r;
		conn->state = STATE_RECEIVING_DATA;
	}
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	size_t req_len = strlen(conn->request_path);
	char *path = calloc(512, 1);

	memmove(path, ".", 1);
	size_t copy_len = 512 - 2;

	if (req_len < copy_len)
		copy_len = req_len;

	memmove(path + 1, conn->request_path, copy_len);
	path[copy_len + 1] = '\0';

	conn->fd = -1;
	conn->fd = open(path, O_RDONLY);
	free(path);

	if (conn->fd < 0)
		return -1;

	struct stat st;

	fstat(conn->fd, &st);
	conn->file_size = st.st_size;
	conn->file_pos = 0;

	return 0;
}

static int aws_on_headers_complete_cb(http_parser *p)
{
	struct connection *conn = (struct connection *)p->data;

	conn->state = STATE_REQUEST_RECEIVED;
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */

	size_t available_space = BUFSIZ - conn->header_len;
	size_t read_size = conn->file_size < available_space
		? conn->file_size : available_space;

	conn->send_len = conn->header_len + read_size;
	conn->send_pos = 0;
	conn->state = STATE_SENDING_DATA;

	close(conn->fd);
	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser p;
	http_parser_settings cfg;

	memset(&cfg, 0, sizeof(http_parser_settings));
	http_parser_init(&p, HTTP_REQUEST);

	p.data = conn;
	cfg.on_path = aws_on_path_cb;
	cfg.on_headers_complete = aws_on_headers_complete_cb;

	size_t consumed =
		http_parser_execute(&p, &cfg, conn->recv_buffer, conn->recv_len);

	return (consumed == conn->recv_len) ? 0 : -1;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	ssize_t sent = sendfile(conn->sockfd, conn->fd, (off_t *)&conn->file_pos,
						conn->file_size - conn->file_pos);

	if (sent < 0)
		return STATE_CONNECTION_CLOSED;

	if (conn->file_pos == conn->file_size)
		return STATE_DATA_SENT;

	return STATE_SENDING_DATA;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	ssize_t sent = send(conn->sockfd, conn->send_buffer + conn->send_pos,
						conn->send_len - conn->send_pos, 0);
	if (sent < 0)
		return -1;

	conn->send_pos += sent;

	if (conn->send_pos == conn->send_len)
		return 1;
	return 0;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	if (connection_send_data(conn) < 0)
		return -1;
	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */

	if (conn->state == STATE_SENDING_HEADER ||
		conn->state == STATE_SENDING_DATA ||
		conn->state == STATE_SENDING_404)
		return;
	receive_data(conn);
	parse_header(conn);

	conn->res_type = connection_get_resource_type(conn);

	if (conn->res_type == RESOURCE_TYPE_NONE ||
		connection_open_file(conn) < 0) {
		connection_prepare_send_404(conn);
		w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
		return;
	}

	connection_prepare_send_reply_header(conn);
	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */
	if (conn->send_pos < conn->send_len) {
		int header_done = connection_send_data(conn);

		if (header_done < 0) {
			w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
			connection_remove(conn);
			return;
		}

		if (header_done == 0)
			return;
	}

	if (conn->fd < 0 || conn->file_size == 0) {
		w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		connection_remove(conn);
		return;
	}

	enum connection_state st = connection_send_static(conn);

	if (st == STATE_DATA_SENT || st == STATE_CONNECTION_CLOSED) {
		close(conn->fd);
		conn->fd = -1;
		w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		connection_remove(conn);
		return;
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	unsigned int in = event & EPOLLIN;
	unsigned int out = event & EPOLLOUT;

	if (in)
		handle_input(conn);
	if (out)
		handle_output(conn);
}

int main(void)
{
	int rc;
	// analizand fisierul epoll_echo_server.c din folderul samples
	// ne dam seama cum sa initilizam serverul

	/* TODO: Initialize asynchronous operations. */
	memset(&ctx, 0, sizeof(ctx));
	rc = io_setup(128, &ctx);
	DIE(rc < 0, "io_setup");

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	efd = eventfd(0, EFD_NONBLOCK);
	DIE(efd < 0, "eventfd");

	rc = w_epoll_add_fd_in(epollfd, efd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT,
		DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* TODO: Add server socket to epoll object*/
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	/* Uncomment the following line for debugging. */
	// dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (5 > 4) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */

		if (rev.data.fd == listenfd) {
			dlog(LOG_DEBUG, "New connection\n");
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			struct connection *conn = rev.data.ptr;

			handle_client(rev.events, conn);
		}
	}

	return 0;
}
