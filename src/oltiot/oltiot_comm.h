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
#include <unordered_map>
#include <chrono>
#include <thread>
#include <atomic>


#define OLTIOT_COMM_SUCCESS              1      //成功
#define OLTIOT_COMM_PARM_ERROR           -10001  //参数错误
#define OLTIOT_COMM_BUSINESS_ERROR       -10002  //业务错误
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


struct RetransmitInfo {
    oltiot_msg_req_t req; // 存储完整的请求消息
    std::chrono::steady_clock::time_point last_sent_time; // 上次发送时间
    std::chrono::steady_clock::time_point first_sent_time; // 首次发送时间
    int retry_count;
};

/**
 * @brief 异步重传管理器 (C++ 风格)
 * 封装了所有重传逻辑，线程安全
 */
class AsyncRetryManager {
public:
    AsyncRetryManager(int min_interval_sec, int increment_sec, int max_interval_sec);
    ~AsyncRetryManager();

    void start();
    void stop();
    void add_for_retry(const oltiot_msg_req_t& req);
    void on_ack_received(const std::string& seq);

private:
    void run_worker();

    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_flag_;
    std::thread worker_thread_;
    std::chrono::seconds min_interval_;    // 最小重试间隔（秒）
    std::chrono::seconds increment_;       // 每次递增的秒数
    std::chrono::seconds max_interval_;    // 最大重试间隔（秒）
    
    std::unordered_map<std::string, RetransmitInfo> in_flight_messages_;
};

extern AsyncRetryManager g_retry_manager;

// ==== 保证送达发送接口 ====
int oltiot_comm_send_guaranteed(oltiot_msg_req_t req); // 故意按值传递来复制

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

// ==== 发送请求消息 ====
int oltiot_send_message(const oltiot_msg_req_t& req);