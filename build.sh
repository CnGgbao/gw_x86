#!/bin/bash

set -e

# 脚本所在目录（项目根目录）
SCRIPT_DIR=$(cd $(dirname "$0") && pwd)

# CMakeLists.txt 路径
CMAKE_DIR="$SCRIPT_DIR/src"

if [ ! -f "$CMAKE_DIR/CMakeLists.txt" ]; then
    echo "Cannot find CMakeLists.txt in $CMAKE_DIR"
    exit 1
fi

# build 目录放在项目根目录
BUILD_DIR="$SCRIPT_DIR/build"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# RapidJSON 头文件目录（你可以根据实际路径修改）
RAPIDJSON_DIR="/home/cyg/mqtt_x86/mqtt.client/rapidjson/include"

# 生成 Makefile，并把 rapidjson include 路径传给 CMake
cmake "$CMAKE_DIR" \
    -DCMAKE_RUNTIME_OUTPUT_DIRECTORY="$BUILD_DIR" \
    -DRAPIDJSON_INCLUDE_DIR="$RAPIDJSON_DIR"

# 并行编译
make -j$(nproc)

# 可执行文件名
EXEC_FILE="$BUILD_DIR/gw_x86"

# 检查是否生成成功
if [ ! -f "$EXEC_FILE" ]; then
    echo "Build failed: $EXEC_FILE not found"
    exit 1
fi

# 运行可执行文件
"$EXEC_FILE"
