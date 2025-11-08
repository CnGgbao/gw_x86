# src/cli/CMakeLists.txt

# 收集当前目录下所有 cpp 文件
file(GLOB CLI_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

# 暴露变量给上层使用
set(CLI_SRC ${CLI_SOURCES} PARENT_SCOPE)

# 如果 cli 有自己的头文件路径，也可以暴露出去
set(CLI_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)
