#include "oltiot.h"
#include <thread>
#include <chrono>
#include "oltiot_comm.h"
#include "oltiot_devobj.h"

// 全局 MQTT 客户端对象
std::shared_ptr<mqtt::async_client> g_mqtt_client = nullptr;
// 全局 MQTT 回调对象
std::shared_ptr<callback> g_cb;

// 保护 MQTT 客户端的互斥锁
std::mutex client_mutex;

// === MQTT 连接参数 ===
const std::string SERVER_ADDRESS(OLTIOT_SERVE_URI);
const std::string CLIENT_ID(OLT_CLIENT_ID);

void oltiot_devobj_init()
{
    // 创建 MQTT 异步客户端
    g_mqtt_client = std::make_shared<mqtt::async_client>(SERVER_ADDRESS, oltiot_devobj_get_did());

    // 设置连接选项
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(120);   // 心跳间隔 120 秒
    connOpts.set_clean_session(true);        // 不持久化会话
    connOpts.set_user_name(OLT_USERNAME);    
    connOpts.set_password(OLT_PASSWORD);     

    // 创建回调对象并绑定到客户端（全局 shared_ptr 保持生命周期）
    // g_cb = std::make_shared<callback>(*g_mqtt_client, connOpts);
    g_cb = std::make_shared<callback>(g_mqtt_client, connOpts);
    g_mqtt_client->set_callback(*g_cb);

    oltiot_devobj_register(); // 注册设备对象处理函数

    // 连接服务器
    try {
        std::cout <<"[oltiot_devobj_init]" << "Connecting to the MQTT server..." << std::flush;
        g_mqtt_client->connect(connOpts, nullptr, *g_cb);
    }
    catch (const mqtt::exception& exc) {
        std::cerr <<"[oltiot_devobj_init]" << "\n❌ ERROR: Unable to connect to MQTT server: "
                  << SERVER_ADDRESS << "\n"
                  << exc.what() << std::endl;
    }

    std::cout <<"[oltiot_devobj_init]" << "\n✅ Running... (press Ctrl+C to exit)" << std::endl;
}

void oltiot_init()
{
    oltiot_devobj_init();
}


//==============================//
//      action_listener 实现
//==============================//

action_listener::action_listener(const std::string& name)
    : name_(name) {}

void action_listener::on_failure(const mqtt::token& tok) {
    std::cout << name_ << " failure";
    if (tok.get_message_id() != 0)
        std::cout << " for token: [" << tok.get_message_id() << "]";
    std::cout << std::endl;
}

void action_listener::on_success(const mqtt::token& tok) {
    std::cout << name_ << " success";
    if (tok.get_message_id() != 0)
        std::cout << " for token: [" << tok.get_message_id() << "]";
    auto top = tok.get_topics();
    if (top && !top->empty())
        std::cout << "\ttoken topic: '" << (*top)[0] << "', ..." << std::endl;
    std::cout << std::endl;
}

//==============================//
//          callback 实现
//==============================//

callback::callback(std::shared_ptr<mqtt::async_client> cli, const mqtt::connect_options& connOpts)
    : nretry_(0), client_ptr_(cli), connOpts_(connOpts), subListener_("Subscription") {}

/**
 * 重连函数：用于连接断开后自动重连 MQTT 服务器
 */
void callback::reconnect() {
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    try {
        std::cout << "[reconnect] Trying to reconnect..." << std::endl;
        client_ptr_->connect(connOpts_, nullptr, *this);
    }
    catch (const mqtt::exception& exc) {
        std::cerr << "[reconnect] Reconnect Error: " << exc.what() << std::endl;
    }
}

void callback::on_failure(const mqtt::token& tok) {
    std::cout << "[on_failure] Connection attempt failed" << std::endl;
    if (++nretry_ > 5)
        return;
    reconnect();
}

void callback::on_success(const mqtt::token& tok) {
    (void)tok;
    std::cout << "[on_failure] Connection success" << std::endl;
}

void callback::connected(const std::string& cause) {
    
    std::cout << "\n[connected] Connected successfully" << std::endl;
    
    if (!cause.empty())
        std::cout << "\tCause: " << cause << std::endl;


    try {
        // 遍历全局注册列表，重新订阅
        std::lock_guard<std::mutex> lock(reg_list_mutex);
        for (auto& node : reg_list) {
            const std::string& topic = node.reg.topic;
            int qos = OLTIOT_COMM_QOS;

            std::cout << "[connected] Subscribing topic: '" << topic 
                      << "' for method: '" << node.reg.method 
                      << "' with QoS " << qos << std::endl;

            // 异步订阅，带 action_listener
            client_ptr_->subscribe(topic, qos, nullptr, subListener_);
        }
    }
    catch (const mqtt::exception& exc) {
        std::cerr << "[connected] Re-subscribe failed: " << exc.what() << std::endl;
    }

    /*eid test*/
    // eid_item_t eids = {"0110030405060707", 1, 8192, 1};
    // oltiot_report_eids(eids);


    /*pid test*/
    // property_item_t prop = {"0110030405060707", {{1,15,10},{1,16,20}}};
    // //property_item_t prop = {"0110030405060707", {{1,15,10}}};
    // oltiot_report_pids(prop);

    /*dev_reg test*/
    // gateway_base_info_t properties = {oltiot_devobj_get_did(), "WG001", 1, "1.0.0", "0x10"};
    // oltiot_gateway_reg(properties);

    /*report dev test*/
    // std::vector<dev_item_t> devs ={{"0120030433060707", "WG001", 1, "1.0.0", "0x10", 1 , 1, 28800}};
    // std::vector<dev_item_t> devs ={{"0120030533060707", "WG001", 1, "1.0.0", "0x10", 1 , 1, 28800},{"0120030633060707", "WG001", 1, "1.0.0", "0x10", 1 , 1, 28800}};
    // oltiot_report_dev(devs);

    /*report dev online test*/
    //std::vector<online_item_t> onlines ={{"0120030433060707", 0}};
    // std::vector<online_item_t> onlines ={{"0120030533060707", 0},{"0120030633060707", 0}};
    // oltiot_report_online(onlines);

    /*report dev del test*/
    // std::vector<did_item_t> dels ={{"0120030433060707"},{"0120030533060707"}};
    // std::vector<did_item_t> dels ={{"0120030633060707"}};
    // oltiot_report_del_dev(dels);
}


void callback::connection_lost(const std::string& cause) {
    std::cout << "\n[connection_lost] Lost connection" << std::endl;
    if (!cause.empty())
        std::cout << "\tCause: " << cause << std::endl;

    std::cout << "[connection_lost] Reconnecting..." << std::endl;
    nretry_ = 0;
    reconnect();
}

void callback::message_arrived(mqtt::const_message_ptr msg) {
    std::cout << "[message_arrived] Message arrived" << std::endl;
    std::cout << "\tTopic: " << msg->get_topic() << std::endl;
    std::cout << "\tPayload: " << msg->to_string() << std::endl;

    // 分发到 oltiot_comm 逻辑
    oltiot_message_arrived(msg->get_topic(), msg->to_string());
}

mqtt::iaction_listener& callback::get_sub_listener()
{
    static action_listener listener("AsyncSub");
    return listener;
}
