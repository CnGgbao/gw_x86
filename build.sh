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

# RapidJSON 头文件目录
RAPIDJSON_DIR="/home/cyg/mqtt_x86/mqtt.client/rapidjson/include"

# ==============================
# 1. 参数解析
# ==============================
RUN_APP=false
CMAKE_EXTRA_ARGS=""

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -r|--run)
            RUN_APP=true
            shift 
            ;;
        -c|--clean)
            echo "Cleaning build directory..."
            rm -rf "$BUILD_DIR"
            shift 
            ;;
        -h|--help)
            echo "Usage: $0 [options] [cmake_args]"
            echo "Options:"
            echo "  -r, --run      Run the executable after build"
            echo "  -c, --clean    Clean build directory before building"
            echo "  -h, --help     Show this help message"
            echo "Examples:"
            echo "  $0             # Build only"
            echo "  $0 -r          # Build and Run"
            echo "  $0 -c -r       # Clean, Build, and Run"
            echo "  $0 -DDEBUG=ON  # Build with extra CMake define"
            exit 0
            ;;
        *)
            # 将其他参数收集起来传给 CMake
            CMAKE_EXTRA_ARGS="$CMAKE_EXTRA_ARGS $1"
            shift
            ;;
    esac
done

# ==============================
# 2. 编译过程
# ==============================

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

echo "Configuring CMake with args: $CMAKE_EXTRA_ARGS"

# 生成 Makefile，并把 rapidjson include 路径传给 CMake
# 注意：$CMAKE_EXTRA_ARGS 放在最后，允许用户覆盖前面的设置
cmake "$CMAKE_DIR" \
    -DCMAKE_RUNTIME_OUTPUT_DIRECTORY="$BUILD_DIR" \
    -DRAPIDJSON_INCLUDE_DIR="$RAPIDJSON_DIR" \
    $CMAKE_EXTRA_ARGS

# 并行编译
make -j$(nproc)

# 可执行文件名
EXEC_FILE="$BUILD_DIR/gw_x86"

# 检查是否生成成功
if [ ! -f "$EXEC_FILE" ]; then
    echo "Build failed: $EXEC_FILE not found"
    exit 1
fi

echo "----------------------------------------"
echo "Build Successful: $EXEC_FILE"

# ==============================
# 3. 运行逻辑 (可选)
# ==============================

if [ "$RUN_APP" = true ]; then
    echo "Running application..."
    echo "----------------------------------------"
    "$EXEC_FILE"
else
    echo "Tip: Use '-r' to run the application after build."
fi
