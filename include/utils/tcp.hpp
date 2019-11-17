//
// Created by xiaol on 11/8/2019.
//

#ifndef SPARKPP_TCP_HPP
#define SPARKPP_TCP_HPP

#include <boost/optional.hpp>
#include <absl/types/span.h>

using absl::Span;

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
// TODO: handle errors
// TODO: replace to boost tcp connection
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

    Span<char> recv(Span<char> s, int flags = 0) {
        int len = ::recv(fd, s.data(), s.length(), flags);
        return Span<char>(s.data(), len);
    }

    int send(Span<char> s, int flags = 0) {
        return ::send(fd, s.data(), s.length(), flags);
    }

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
        int sockfd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in addr{
            .sin_family = AF_INET,
            .sin_port = htons(port)
        };
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        ::bind(sockfd, reinterpret_cast<sockaddr*>(&addr), sizeof(sockaddr_in));
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
