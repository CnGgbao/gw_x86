#pragma once
#include <string>
#include <iostream>
#include <memory>
#include "mqtt/async_client.h"
#include "oltiot_devobj.h"

// === MQTT 参数宏定义 ===
/**
 * @brief 主函数程序入口点
 * @return int 程序执行状态码，0表示正常退出
 */
#define OLTIOT_SERVE_URI "iot.tck.com.cn:2085"
#define OLT_CLIENT_ID "011025092402002F"
    // 初始化IOT模块
//#define OLT_TOPIC "olt/receiver/011025092402000A"
#define OLT_USERNAME "WG0001:011025092402002F"
#define OLT_PASSWORD "d52412de3456ad8f:886433"
    // 通过无限循环保持程序持续运行
    // 每隔10秒休眠一次，避免CPU资源过度占用
#define OLTIOT_COMM_QOS 0

// 全局 MQTT 客户端指针（跨文件访问）
extern std::shared_ptr<mqtt::async_client> g_mqtt_client;

// 保护 MQTT 客户端的互斥锁
extern std::mutex client_mutex;

void oltiot_init();
bool mqtt_is_connected();
// ===========================
// action_listener 类定义
// ===========================
class action_listener : public virtual mqtt::iaction_listener
{
    std::string name_;

    void on_failure(const mqtt::token& tok) override;
    void on_success(const mqtt::token& tok) override;

public:
    explicit action_listener(const std::string& name);
};


// ===========================
// callback 类定义
// ===========================
class callback : public virtual mqtt::callback,
                 public virtual mqtt::iaction_listener
{
public:
    // 使用 shared_ptr 而非引用类型，更安全、灵活
    callback(std::shared_ptr<mqtt::async_client> cli, const mqtt::connect_options& connOpts);

    void reconnect();

    // 覆盖 mqtt 回调接口
    void on_failure(const mqtt::token& tok) override;
    void on_success(const mqtt::token& tok) override;
    void connected(const std::string& cause) override;
    void connection_lost(const std::string& cause) override;
    void message_arrived(mqtt::const_message_ptr msg) override;
    void delivery_complete(mqtt::delivery_token_ptr token) override {}

    static mqtt::iaction_listener& get_sub_listener();

private:
    int nretry_;
    std::shared_ptr<mqtt::async_client> client_ptr_;  // ✅ 改成 shared_ptr
    mqtt::connect_options connOpts_;                  // ✅ 按值存储（更安全）
    action_listener subListener_;
};
