#include "oltiot_comm.h"
#include "oltiot.h"
#include <thread>
#include <vector>
#include <rapidjson/document.h>
#include <random>
#include <sstream>
#include <iomanip>
#include <string>

std::mutex reg_list_mutex;
std::list<oltiot_comm_reg_node_t> reg_list;

// 参数: 最小间隔(5s), 递增量(5s), 最大间隔(60s)
AsyncRetryManager g_retry_manager(5, 5, 60);

void oltiot_msg_handle_reg(const oltiot_msg_reg_t* reg, oltiot_msg_reg_cb cb, void* arg)
{
    if (!reg || !cb || reg->topic.empty() || reg->method.empty()) return;

    auto new_node = std::make_shared<oltiot_comm_reg_node_t>();
    new_node->reg = *reg;
    new_node->cb = cb;
    new_node->arg = arg;
    new_node->filter_cnt = 0;
    new_node->filter_keys.clear();
    new_node->filter_values.clear();

    // ===== 解析 filter =====
    if (!reg->filter.empty()) {
        rapidjson::Document doc;
        if (doc.Parse(reg->filter.c_str()).HasParseError() || !doc.IsObject()) {
            std::cerr << "[oltiot] ⚠️ Filter format error for method " << reg->method << std::endl;
            return;
        }

        for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
            if (it->name.IsString()) {
                new_node->filter_keys.emplace_back(it->name.GetString());
                if (it->value.IsString()) {
                    new_node->filter_values.emplace_back(it->value.GetString());
                } else {
                    new_node->filter_values.emplace_back(""); // 非字符串默认空
                }
                new_node->filter_cnt++;
            }
        }
    }

    // ===== MQTT 已连接，立即订阅 =====
    if (g_mqtt_client && g_mqtt_client->is_connected()) {
        try {
            g_mqtt_client->subscribe(
                reg->topic,
                OLTIOT_COMM_QOS,
                nullptr,
                callback::get_sub_listener()
            );
            std::cout << "[oltiot] 🚀 Async subscribe sent: " << reg->topic << std::endl;
        }
        catch (const mqtt::exception& exc) {
            std::cerr << "[oltiot] ❌ Failed to send subscribe: " << reg->topic
                      << ", error: " << exc.what() << std::endl;
        }
    } else {
        std::cout << "[oltiot] ℹ️ Client not connected yet, will subscribe on connect." << std::endl;
    }

    // ===== 插入注册列表 =====
    std::lock_guard<std::mutex> lock(reg_list_mutex);

    if (new_node->filter_cnt == 0) {
        // filter 为空，直接放尾部
        reg_list.push_back(*new_node);
    } else {
        // filter 有值，按 filter_cnt 降序插入
        auto it = reg_list.begin();
        for (; it != reg_list.end(); ++it) {
            if (new_node->filter_cnt > it->filter_cnt) break;
        }
        reg_list.insert(it, *new_node);
    }

    std::cout << "[oltiot] ✅ Registered handle: " << reg->method
              << " for topic " << reg->topic
              << (new_node->filter_cnt ? " with filter" : "") << std::endl;
}

int oltiot_message_arrived(const std::string& topicName, const std::string& payload)
{
    using namespace rapidjson;

    Document doc;
    if (doc.Parse(payload.c_str()).HasParseError()) {
        std::cout << "[oltiot] JSON parse error" << std::endl;
        return 1;
    }

    bool isAck = doc.HasMember("result");

    auto msg = std::make_shared<oltiot_msg_resp_t>();
    msg->topic  = topicName;
    msg->is_ack = isAck;   // ✅ 保存 ACK 状态

    auto getStr = [&](const char* key) -> std::string {
        if (doc.HasMember(key) && doc[key].IsString())
            return doc[key].GetString();
        return "";
    };
    auto getInt = [&](const char* key) -> int {
        if (doc.HasMember(key) && doc[key].IsInt())
            return doc[key].GetInt();
        return 0;
    };

    msg->method = getStr("method");
    msg->src    = getStr("src");
    msg->dst    = getStr("dst");
    msg->seq    = getStr("seq");

    // params 提取
    bool hasParams = false;
    if (doc.HasMember("params") && doc["params"].IsObject()) {
        auto d = std::make_shared<Document>();
        d->CopyFrom(doc["params"], d->GetAllocator());
        msg->params = d;
        hasParams = true;
    }

    if (isAck && !msg->seq.empty()) {
        std::cout << "[oltiot_comm] ACK received for SEQ: " << msg->seq << std::endl;
        
        g_retry_manager.on_ack_received(msg->seq);
    }

    // result 提取（仅 ACK）
    if (isAck) {
        msg->result = getInt("result");
        std::cout << "[oltiot] Ack message received for method: " << msg->method << std::endl;
    }

    // 只过滤“纯 ACK”（即没有 params 的 ACK）
    if (isAck && !hasParams) {
        std::cout << "[oltiot] Pure ACK (no params), skip callback" << std::endl;
        return 1;
    }

    std::cout << "[oltiot] Distributing message for method: " << msg->method << std::endl;

    std::lock_guard<std::mutex> lock(reg_list_mutex);
    for (auto& node : reg_list) {
        //std::cout<<"node.reg.method: "<<node.reg.method<<", msg->method: "<<msg->method<<std::endl;
        if (node.reg.method != msg->method)
            continue;

        bool match = false;

        if (node.filter_cnt == 0) {
            match = true;
            std::cout << "[oltiot] Find listener (no filter) for method: " << msg->method << std::endl;
        } else {
            match = true;
            for (size_t i = 0; i < node.filter_keys.size(); ++i) {
                const std::string& key = node.filter_keys[i];
                const std::string& val = node.filter_values[i];

                if (!msg->params || !msg->params->HasMember(key.c_str())) {
                    match = false;
                    break;
                }

                const Value& v = (*msg->params)[key.c_str()];
                if (!val.empty() && (!v.IsString() || v.GetString() != val)) {
                    match = false;
                    break;
                }
            }

            if (match) {
                std::cout << "[oltiot] Find listener for method: " << msg->method
                          << " (filter matched)" << std::endl;
            }
        }

        if (match) {
            std::thread([cb = node.cb, msg_copy = msg, arg = node.arg]() {
                oltiot_msg_req_t req;
                req.topic  = msg_copy->topic;
                req.method = msg_copy->method;
                req.src    = msg_copy->src;
                req.dst    = msg_copy->dst;
                req.seq    = msg_copy->seq;
                req.params = msg_copy->params;
                req.is_ack = msg_copy->is_ack;

                cb(&req, arg);
            }).detach();
            break;
        }
    }

    return 1;
}



void oltiot_msg_resp_new(const oltiot_msg_req_t *from_req, oltiot_msg_resp_t *to_resp)
{
    memset(to_resp, 0, sizeof(*to_resp));
    to_resp->method = from_req->method;
    to_resp->src    = from_req->dst;
    to_resp->dst    = from_req->src;
    to_resp->seq    = from_req->seq;
    to_resp->result = 1;
}

int oltiot_msg_resp(const oltiot_msg_resp_t* to_resp) 
{
    if (!to_resp || to_resp->topic.empty()) {
        return -1;
    }

    // 构建 JSON
    rapidjson::Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    doc.AddMember("method", rapidjson::Value(to_resp->method.c_str(), alloc), alloc);
    doc.AddMember("src", rapidjson::Value(to_resp->src.c_str(), alloc), alloc);
    doc.AddMember("dst", rapidjson::Value(to_resp->dst.c_str(), alloc), alloc);
    doc.AddMember("result", to_resp->result, alloc);
    doc.AddMember("seq", rapidjson::Value(to_resp->seq.c_str(), alloc), alloc);

    // 复制 params
    if (to_resp->params) {
        rapidjson::Value params_copy(*to_resp->params, alloc);
        doc.AddMember("params", params_copy, alloc);
    }

    // 序列化为字符串
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    std::string json_string = buffer.GetString();

    std::cout << "[DEBUG] TOPIC: " << to_resp->topic << "\nRSP: " << json_string << std::endl;

    // 构造 MQTT 消息
    auto pubmsg = mqtt::make_message(to_resp->topic, json_string);
    pubmsg->set_qos(0);      // 默认 QoS，可以根据需求改

    // 异步发送，线程安全
    int rc = -1;
    {
        std::lock_guard<std::mutex> lock(client_mutex);
        if (g_mqtt_client) {
            try {
                g_mqtt_client->publish(pubmsg);
                rc = 0; // 成功
            } catch (const mqtt::exception& e) {
                std::cerr << "[ERROR] MQTT publish failed: " << e.what() << std::endl;
                rc = -1;
            }
        }
    }

    return rc;
}


std::string generate_seq() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());                  // 随机引擎
    static std::uniform_int_distribution<uint64_t> dis(0, 0xFFFFFFFFFFFFFFFF);

    uint64_t rand_val = dis(gen);                     // 生成 64 位随机数
    std::stringstream ss;
    ss << std::hex << std::setw(16) << std::setfill('0') << rand_val;
    return ss.str().substr(0, 16);                    // 取前 16 位
}

int oltiot_send_message(const oltiot_msg_req_t& req)
{
    using namespace rapidjson;

    // ===== 构建 JSON =====
    Document doc;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    // 基本字段
    doc.AddMember("method", Value(req.method.c_str(), alloc), alloc);
    doc.AddMember("src", Value(req.src.c_str(), alloc), alloc);
    doc.AddMember("dst", Value(req.dst.c_str(), alloc), alloc);
    doc.AddMember("ver", Value(req.ver.c_str(), alloc), alloc);

    // 可选 params
    if (req.params) {
        Value params_copy;
        params_copy.CopyFrom(*req.params, alloc);
        doc.AddMember("params", params_copy, alloc);
    }

    if(req.seq.empty())
    {
        return OLTIOT_COMM_PARM_ERROR;
    }

    // seq
    std::string seq = req.seq.empty() ? generate_seq() : req.seq;
    doc.AddMember("seq", Value().SetString(seq.c_str(), alloc), alloc);
    // ===== 序列化 JSON =====
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    doc.Accept(writer);
    std::string json_string = buffer.GetString();

    std::cout << "[DEBUG] JSON: " << json_string << std::endl;

    // ===== 构造 MQTT 消息 =====
    auto pubmsg = mqtt::make_message(req.topic, json_string);
    pubmsg->set_qos(0);      // QoS 可根据需求
    pubmsg->set_retained(false);

    // ===== 异步发送，线程安全 =====
    int rc = -1;
    {
        std::lock_guard<std::mutex> lock(client_mutex);
        if (g_mqtt_client) {
            try {
                g_mqtt_client->publish(pubmsg);
                rc = 0; // 成功
                std::cout << "[DEBUG] MQTT publish success" << std::endl;
            } catch (const mqtt::exception& e) {
                std::cerr << "[ERROR] MQTT publish failed: " << e.what() << std::endl;
                rc = -1;
            }
        } else {
            std::cerr << "[WARN] MQTT client not connected!" << std::endl;
        }
    }

    return rc;
}

AsyncRetryManager::AsyncRetryManager(int min_interval_sec, int increment_sec, int max_interval_sec)
    : stop_flag_(false), 
      min_interval_(min_interval_sec),
      increment_(increment_sec),
      max_interval_(max_interval_sec)
{
    // 确保最小间隔不大于最大间隔
    if (min_interval_ > max_interval_) {
        min_interval_ = max_interval_;
    }
}

AsyncRetryManager::~AsyncRetryManager() {
    stop();
}

void AsyncRetryManager::start() {
    if (worker_thread_.joinable()) {
        return; // 已经启动
    }
    std::cout << "[RetryManager] Starting C++ style retry manager thread..." << std::endl;
    worker_thread_ = std::thread(&AsyncRetryManager::run_worker, this);
}

void AsyncRetryManager::stop() {
    stop_flag_.store(true);
    cv_.notify_one(); // 唤醒工作线程以便退出
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    std::cout << "[RetryManager] Stopped." << std::endl;
}

/**
 * @brief 后台重传线程
 */
void AsyncRetryManager::run_worker() {
    // 循环间隔
    const auto loop_interval = std::chrono::seconds(5);

    while (!stop_flag_.load()) {
        
        std::vector<oltiot_msg_req_t> messages_to_resend;
        auto now = std::chrono::steady_clock::now();

        // 1. 检查哪些消息需要重传
        {
            std::unique_lock<std::mutex> lock(mutex_); 
            for (auto it = in_flight_messages_.begin(); it != in_flight_messages_.end(); ++it) {
                
                long long current_delay_sec = min_interval_.count() + (it->second.retry_count * increment_.count());
                
                if (current_delay_sec > max_interval_.count()) {
                    current_delay_sec = max_interval_.count();
                }
                
                // 构造 chrono::seconds 对象用于比较
                std::chrono::seconds required_delay(current_delay_sec);

                auto time_since_sent = now - it->second.last_sent_time;
                
                if (time_since_sent >= required_delay) { // ✅ 使用动态计算的间隔
                    std::cout << "[RetryManager] Resending message for method: " 
                              << it->second.req.method << ", SEQ: " << it->second.req.seq 
                              << ". Delay: " << required_delay.count() << "s" 
                              << ". Count: " << it->second.retry_count + 1 << std::endl;
                    
                    messages_to_resend.push_back(it->second.req);
                    
                    it->second.last_sent_time = now;
                    it->second.retry_count++;
                }
            }
        } // 互斥锁在这里释放

        // 2. 执行重传（在锁外发送，避免死锁）
        for (const auto& req : messages_to_resend) {
            oltiot_send_message(req); // 调用底层的发送函数
        }

        // 3. 高效休眠，直到被 stop() 唤醒或超时
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, loop_interval, [this]{ return stop_flag_.load(); });
    }
}


void AsyncRetryManager::add_for_retry(const oltiot_msg_req_t& req) {
    RetransmitInfo info;
    info.req = req; // 存储副本
    info.last_sent_time = std::chrono::steady_clock::now();
    info.retry_count = 0; // ✅ 第一次发送（未重试），计数为 0

    {
        std::lock_guard<std::mutex> lock(mutex_);
        in_flight_messages_[req.seq] = info;
    }
    
    std::cout << "[RetryManager] Added message for retry. Method: " 
              << req.method << ", SEQ: " << req.seq << std::endl;
}


void AsyncRetryManager::on_ack_received(const std::string& seq) {
    if (seq.empty()) return;

    std::lock_guard<std::mutex> lock(mutex_);
    
    if (in_flight_messages_.erase(seq) > 0) {
        std::cout << "[RetryManager] ACK matched. Removed SEQ: " << seq << " from retry map." << std::endl;
    }
}

int oltiot_comm_send_guaranteed(oltiot_msg_req_t req) // 按值传递，获取副本
{
    g_retry_manager.add_for_retry(req);

    return oltiot_send_message(req);
}