# src/oltiot/CMakeLists.txt

# 收集当前目录下所有 cpp 文件
file(GLOB OLTIOT_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

# 暴露变量给上层使用
set(OLTIOT_SRC ${OLTIOT_SOURCES} PARENT_SCOPE)

# 如果 oltiot 有自己的头文件路径，也可以暴露出去
set(OLTIOT_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)
