#include <iostream>
#include <cassert>
#include "../server/resp_parser.h"

using namespace minkv::server;

int main() {
    std::cout << "Starting RESP Parser Tests..." << std::endl;

    // Test 1: Parse normal SET command
    // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
    std::string raw = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    auto result = RespParser::parse(raw);
    
    assert(result.has_value());
    assert(result->size() == 3);
    assert(result->at(0) == "SET");
    assert(result->at(1) == "key");
    assert(result->at(2) == "value");
    std::cout << "[Pass] Parse SET command" << std::endl;

    // Test 2: Incomplete data (Half package)
    // *3\r\n$3\r\nSET... (missing rest)
    std::string partial = "*3\r\n$3\r\nSET"; 
    auto res_partial = RespParser::parse(partial);
    assert(!res_partial.has_value());
    std::cout << "[Pass] Handle incomplete data" << std::endl;

    // Test 3: Serialize
    assert(RespParser::serialize_simple_string("OK") == "+OK\r\n");
    assert(RespParser::serialize_bulk_string("hello") == "$5\r\nhello\r\n");
    std::cout << "[Pass] Serialization" << std::endl;

    std::cout << "All parser tests passed!" << std::endl;
    return 0;
}
