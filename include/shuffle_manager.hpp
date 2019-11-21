//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SHUFFLE_MANAGER_HPP
#define SPARKPP_SHUFFLE_MANAGER_HPP

#include "common.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <fmt/format.h>


namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

struct ShuffleManager {
    string localDir;
    string shuffleDir;
    string serverUri;
    ShuffleManager() : localDir{"/tmp/sparkpp"}, shuffleDir{"/tmp/sparkpp/shuffle"} {
        fs::create_directories(localDir);
        fs::create_directories(shuffleDir);
        char* localIp = std::getenv("SPARK_LOCAL_IP");
        serverUri = fmt::format("{}", localIp);
        thread thd{[localDir = localDir]() {
            uint16_t port = 28001;
            net::io_context ioc;
            tcp::acceptor acceptor{ioc, {ip::tcp::v4(), port}};
            while (true) {
                tcp::socket socket{ioc};
                acceptor.accept(socket);
                thread per_conn([localDir = localDir, socket = move(socket)]() mutable {
                    beast::flat_buffer buffer;
                    beast::error_code ec;
                    http::request<http::string_body> req;
                    http::read(socket, buffer, req, ec);
                    // TODO: replace this to in-memory shuffle cache?
                    string path = localDir + req.target().to_string();
                    http::file_body::value_type body;
                    body.open(path.c_str(), beast::file_mode::scan, ec);
                    http::response<http::file_body> res{
                        std::piecewise_construct,
                        make_tuple(move(body)),
                        make_tuple(http::status::ok, req.version())
                    };
                    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
                    http::write(socket, res);
                    socket.shutdown(tcp::socket::shutdown_both, ec);
                });
                per_conn.detach();
            }
        }};
        thd.detach();
    }


};


#endif //SPARKPP_SHUFFLE_MANAGER_HPP
