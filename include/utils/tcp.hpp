//
// Created by xiaol on 11/8/2019.
//

#ifndef SPARKPP_TCP_HPP
#define SPARKPP_TCP_HPP

#include <boost/optional.hpp>

#ifdef __WIN32__
#include <winsock2.h>
#pragma comment(lib, "ws2_32")

static auto wsa_init = []() {
    WSADATA d;
    WORD sockVer = MAKEWORD(2, 2);
    return WSAStartup(sockVer, &d);
}();

#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"

struct TcpStream {
    int fd;
    static boost::optional<TcpStream> connect(const char* addr, uint16_t port) {
        int sockfd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in sAddr {
            .sin_family = AF_INET,
            .sin_port = htons(port),
        };
        inet_pton(AF_INET, addr, &sAddr.sin_addr);
        [[maybe_unused]] int msgfd = ::connect(sockfd, reinterpret_cast<sockaddr*>(&sAddr), sizeof(sockaddr_in));
        return {
            msgfd == 0,
            TcpStream{sockfd}
        };
    }

    TcpStream(int fd_) noexcept : fd{fd_} {}
    TcpStream(const TcpStream&) = delete;
    TcpStream(TcpStream&& rhs) noexcept : fd{rhs.fd} {
        rhs.fd = 0;
    }
    TcpStream& operator=(const TcpStream&) = delete;
    TcpStream& operator=(TcpStream&&) = default;

    ~TcpStream() {
#ifdef __WIN32__
        int how = SD_BOTH;
#else
        int how = SHUT_RDWR;
#endif
        if (fd)
            ::shutdown(fd, how);
    }
};



// TODO: handle errors
struct TcpListener {
    int sockfd;
    uint16_t port;
    sockaddr_in addr;
    static TcpListener bind(uint16_t port, uint16_t bufferSize = 10) {
        int option = 1;
        int sockfd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&option, sizeof(option));
        sockaddr_in addr{
            .sin_family = AF_INET,
            .sin_port = htons(port)
        };
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        [[maybe_unused]] int success = ::bind(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(sockaddr_in));
        ::listen(sockfd, bufferSize);

#ifdef __WIN32__
        int len = sizeof(sockaddr_in);
#else
        socklen_t len = sizeof(sockaddr_in);
#endif
        ::getsockname(sockfd, reinterpret_cast<sockaddr*>(&addr), &len);
        port = ntohs(addr.sin_port);
        return TcpListener{
            .sockfd = sockfd,
            .port = port,
            .addr = addr
        };
    }

    TcpStream accept() {
        sockaddr_in addr;
#ifdef __WIN32__
        int len = sizeof(sockaddr_in);
#else
        socklen_t len = sizeof(sockaddr_in);
#endif
        int msgfd = ::accept(sockfd, reinterpret_cast<sockaddr*>(&addr), &len);
        return TcpStream{msgfd};
    }
};
#pragma GCC diagnostic pop



#endif //SPARKPP_TCP_HPP
