#pragma once

#include <string>
#include <mutex>
#include <condition_variable>
#include <list>
#include <memory>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include "mqtt/async_client.h"



#define OLTIOT_COMM_SUCCESS              1      //成功
#define OLTIOT_COMM_PARM_ERROR           10001  //参数错误
#define OLTIOT_COMM_BUSINESS_ERROR       10002  //业务错误
#define OLTIOT_COMM_FAILURE OLTIOT_COMM_BUSINESS_ERROR


// ==== 消息结构 ====
struct oltiot_msg_req_t {
    std::string topic;
    std::string method;
    std::string src;
    std::string dst;
    std::string ver;
    std::string seq;
    std::shared_ptr<rapidjson::Document> params; // ✅ 使用 shared_ptr
    bool is_ack = false;
};

struct oltiot_msg_resp_t {
    std::string topic;
    std::string method;
    std::string src;
    std::string dst;
    std::string seq;
    std::shared_ptr<rapidjson::Document> params; // ✅ 使用 shared_ptr
    int result = 0;
    bool is_ack = false;
};




// 回调类型
using oltiot_msg_reg_cb = void(*)(const oltiot_msg_req_t*, void*);
using oltiot_msg_resp_cb = void(*)(const oltiot_msg_resp_t*, void*);

// 注册结构
struct oltiot_msg_reg_t {
    std::string topic;
    std::string method;
    std::string filter;
};

// 注册节点
struct oltiot_comm_reg_node_t {
    oltiot_msg_reg_t reg;
    oltiot_msg_reg_cb cb = nullptr;
    void* arg = nullptr;
    int filter_cnt = 0;
    std::vector<std::string> filter_keys;    // filter 的 key
    std::vector<std::string> filter_values;  // filter 的 value
};

// 请求节点
struct oltiot_comm_req_node_t {
    std::mutex mutex;
    std::condition_variable cond;
    std::shared_ptr<oltiot_msg_req_t> req;
    std::shared_ptr<oltiot_msg_resp_t> resp;
    bool is_async = false;
    oltiot_msg_resp_cb async_cb = nullptr;
    void* arg = nullptr;
};

// 请求处理任务
struct oltiot_comm_req_process_t {
    std::shared_ptr<oltiot_msg_req_t> req;
    oltiot_msg_reg_cb cb = nullptr;
    void* arg = nullptr;
};

extern std::mutex reg_list_mutex;
extern std::list<oltiot_comm_reg_node_t> reg_list;

// ==== 注册消息处理函数 ====
void oltiot_msg_handle_reg(const oltiot_msg_reg_t* reg, oltiot_msg_reg_cb cb, void* arg);

// ==== 消息到达处理 ====
int oltiot_message_arrived(const std::string& topicName, const std::string& payload);

// ==== 目标地址源地址转换 ====
void oltiot_msg_resp_new(const oltiot_msg_req_t *from_req, oltiot_msg_resp_t *to_resp);

//==== 发送响应消息 ====
int oltiot_msg_resp(const oltiot_msg_resp_t* to_resp) ;

// ==== 生成唯一序列号 ====
std::string generate_seq();