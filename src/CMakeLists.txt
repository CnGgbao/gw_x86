cmake_minimum_required(VERSION 3.10)
project(gw_x86)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ==== OpenSSL 配置 ====
set(OPENSSL_ROOT_DIR "/usr/local/openssl-x86")
set(OPENSSL_INCLUDE_DIR "${OPENSSL_ROOT_DIR}/include")
set(OPENSSL_LIBRARIES "${OPENSSL_ROOT_DIR}/lib64/libssl.a;${OPENSSL_ROOT_DIR}/lib64/libcrypto.a")

# ==== Paho MQTT C/C++ 配置 ====
set(PAHO_MQTT_ROOT "/usr/local/paho-mqtt-cpp-x86")
set(PAHO_MQTT_INCLUDE_DIR "${PAHO_MQTT_ROOT}/include")
set(PAHO_MQTT_LIBS "${PAHO_MQTT_ROOT}/lib/libpaho-mqttpp3.a;${PAHO_MQTT_ROOT}/lib/libpaho-mqtt3a.a")

# ==== RapidJSON 配置 ====
# 支持从外部传入路径（脚本传 -DRAPIDJSON_INCLUDE_DIR）
if(NOT DEFINED RAPIDJSON_INCLUDE_DIR)
    # 如果脚本没有传，使用默认路径（可根据你实际情况修改）
    set(RAPIDJSON_INCLUDE_DIR "/home/cyg/mqtt_x86/mqtt.client/rapidjson/include")
endif()

# ==== 包含头文件 ====
include_directories(
    ${OPENSSL_INCLUDE_DIR}
    ${PAHO_MQTT_INCLUDE_DIR}
    ${RAPIDJSON_INCLUDE_DIR}         # ✅ 加入 RapidJSON
    ${CMAKE_CURRENT_SOURCE_DIR}      # 项目自身头文件
)

# ==== 源文件列表 ====
file(GLOB_RECURSE SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

# ==== 可执行文件 ====
add_executable(gw_x86 ${SOURCES})

# ==== 链接库 ====
target_link_libraries(gw_x86
    ${PAHO_MQTT_LIBS}
    ${OPENSSL_LIBRARIES}
    pthread
)

# ==== 打印信息 ====
message(STATUS "Using RapidJSON include dir: ${RAPIDJSON_INCLUDE_DIR}")
