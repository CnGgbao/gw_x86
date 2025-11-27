#include "oltiot_devobj.h"
#include "oltiot.h"
#include <iostream>
#include <string>
#include <oltiot_comm.h>
#include <mutex>

int oltiot_read_pids(const std::string& did, int sid, int pid, value_entry_t& out_value);
int oltiot_write_pids(std::string did, int sid, int pid, const value_entry_t& val);
int oltiot_do_fids(std::string did, int sid, int fid, int val);
int oltiot_dev_disc(int type, int duration);
int oltiot_dev_stop_disc();
int oltiot_dev_add_sub(std::vector< std::string > did_list, int duration);
int oltiot_dev_del_sub(std::string did);
int oltiot_dev_del_allsub();
int oltiot_dev_gettime(long long timestamp);
std::vector<dev_item_t> get_all_sub_dev_info();


static std::string g_latest_join_seq;
static std::mutex g_seq_mutex;


void set_latest_join_seq(const std::string& seq) {
    std::lock_guard<std::mutex> lock(g_seq_mutex);
    g_latest_join_seq = seq;
}
std::string get_latest_join_seq() {
    std::lock_guard<std::mutex> lock(g_seq_mutex);
    return g_latest_join_seq;
}

void oltiot_ack_resp(const oltiot_msg_req_t* req, int result) {
    oltiot_msg_resp_t to_resp;
    oltiot_msg_resp_new(req, &to_resp);
    
    to_resp.topic  = "olt/receiver/" + req->src; 
    to_resp.result = result;
    to_resp.params = NULL; // 根据您的要求，这里设置为 NULL

    oltiot_msg_resp(&to_resp);
}

// 示例回调函数
void example_cb(const oltiot_msg_req_t* req, void* arg) {
    std::cout << "[example_cb] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    if (!req || !req->params) return;
    int result = OLTIOT_COMM_SUCCESS;
    // default_ack(result); // 发送默认 ACK

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    req->params->Accept(writer);  // 序列化整个 JSON 对象

    std::cout << "[example_cb] Received params: " << buffer.GetString() << std::endl;
}

void readpids_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[readpids_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req || !req->params) {
        std::cerr << "[readpids_handle] Invalid request: params is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    auto& doc = *req->params;

    // 解析 did
    if (!doc.HasMember("did") || !doc["did"].IsString()) {
        std::cerr << "[ReadPids] Missing or invalid 'did'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    std::string did = doc["did"].GetString();

    // 解析 pids[]
    if (!doc.HasMember("pids") || !doc["pids"].IsArray()) {
        std::cerr << "[ReadPids] Missing or invalid 'pids' array" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    const auto& pids = doc["pids"].GetArray();
    std::cout << "[readpids_handle] DID=" << did << " read count=" << pids.Size() << std::endl;


    // ====================== 构造响应 JSON ======================
    auto resp_params = std::make_shared<Document>();
    resp_params->SetObject();
    auto& alloc = resp_params->GetAllocator();

    // 1️⃣ add did
    Value did_val;
    did_val.SetString(did.c_str(), alloc);
    resp_params->AddMember("did", did_val, alloc);

    // 2️⃣ build pids array
    Value pids_arr(kArrayType);

    for (const auto& item : pids) {
        if (!item.HasMember("sid") || !item["sid"].IsInt() ||
            !item.HasMember("pid") || !item["pid"].IsInt()) {
            std::cerr << "[ReadPids] Skip invalid entry" << std::endl;
            continue;
        }

        int sid = item["sid"].GetInt();
        int pid = item["pid"].GetInt();

        value_entry_t val_entry;
        result = oltiot_read_pids(did, sid, pid, val_entry);

        Value obj(kObjectType);
        obj.AddMember("sid", sid, alloc);
        obj.AddMember("pid", pid, alloc);
        switch (val_entry.type) {
            case VALUE_TYPE_INT:
            {
                int int_val = std::stoi(val_entry.value);
                obj.AddMember("val", int_val, alloc);
                break;
            }
            case VALUE_TYPE_STRING:
            {
                obj.AddMember("val", Value(val_entry.value.c_str(), alloc), alloc);
                break;
            }
 
        }

        pids_arr.PushBack(obj, alloc);
    }

    // 加入 params->"pids"
    resp_params->AddMember("pids", pids_arr, alloc);

    
    // ====================== 发送响应 ======================
    oltiot_msg_resp_t to_resp;
    oltiot_msg_resp_new(req, &to_resp);
    
    to_resp.topic  = "olt/receiver/" + req->src; 
    to_resp.result = result;
    to_resp.params = resp_params; 

    oltiot_msg_resp(&to_resp);
}

void writepids_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[writepids_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req || !req->params) {
        std::cerr << "[writepids_handle] Invalid request: params is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    auto& doc = *req->params;

    // 解析 did
    if (!doc.HasMember("did") || !doc["did"].IsString()) {
        std::cerr << "[writepids_handle] Missing or invalid 'did'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    std::string did = doc["did"].GetString();

    //解析 sid
    if (!doc.HasMember("sid") || !doc["sid"].IsInt()) {
        std::cerr << "[writepids_handle] Missing or invalid 'sid'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    int sid = doc["sid"].GetInt();

    // 解析 pids[]
    if (!doc.HasMember("pids") || !doc["pids"].IsArray()) {
        std::cerr << "[writepids_handle] Missing or invalid 'pids' array" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    const auto& pids = doc["pids"].GetArray();
    std::cout << "[writepids_handle] DID=" << did <<" SID=" << sid<< " read count=" << pids.Size() << std::endl;

    for (const auto& item : pids) {
        if (!item.HasMember("pid") || !item["pid"].IsInt() ||
            !item.HasMember("val") || !item["val"].IsInt()) {
            std::cerr << "[writepids_handle] Skip invalid entry" << std::endl;
            continue;
        }

        int pid = item["pid"].GetInt();
        const auto& v = item["val"];
        value_entry_t pv;

        if (v.IsInt()) {
            pv.type = VALUE_TYPE_INT;
            pv.value = std::to_string(v.GetInt());
        } else if (v.IsDouble()) {
            pv.type = VALUE_TYPE_DOUBLE;
            pv.value = std::to_string(v.GetDouble());
        } else if (v.IsBool()) {
            pv.type = VALUE_TYPE_BOOL;
            pv.value = v.GetBool() ? "true" : "false";
        } else if (v.IsString()) {
            pv.type = VALUE_TYPE_STRING;
            pv.value = v.GetString();
        } else {
            pv.type = VALUE_TYPE_UNKNOWN;
            pv.value = "";
        }
        result = oltiot_write_pids(did, sid, pid, pv);
    }

    // ====================== 发送响应 ======================
    oltiot_ack_resp(req, result);
}

void dofids_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[dofids_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req || !req->params) {
        std::cerr << "[dofids_handle] Invalid request: params is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    auto& doc = *req->params;

    // 解析 did
    if (!doc.HasMember("did") || !doc["did"].IsString()) {
        std::cerr << "[dofids_handle] Missing or invalid 'did'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    std::string did = doc["did"].GetString();

    //解析 sid
    if (!doc.HasMember("sid") || !doc["sid"].IsInt()) {
        std::cerr << "[dofids_handle] Missing or invalid 'sid'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    int sid = doc["sid"].GetInt();

    //解析 fid
    if( !doc.HasMember("fid") || !doc["fid"].IsInt()) {
        std::cerr << "[dofids_handle] Missing or invalid 'fid'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    int fid = doc["fid"].GetInt();

    //解析 val
    if (!doc.HasMember("val") || !doc["val"].IsInt()) {
        std::cerr << "[dofids_handle] Missing or invalid 'val'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    int val = doc["val"].GetInt();

        
    result = oltiot_do_fids(did, sid, fid, val);

    // ====================== 发送响应 ======================
    oltiot_ack_resp(req, result);

    // std::this_thread::sleep_for(std::chrono::seconds(3)); 
    
    // std::string did1 = "011125092403004F";

    // pid_item_t item;
    // item.sid = 1;
    // item.pid = 61;
    // item.val.type = VALUE_TYPE_INT;
    // item.val.value = "255";

    // std::vector<pid_item_t> pids;
    // pids.push_back(item);


    // property_item_t prop = {did1, pids};
    // oltiot_report_pids(prop);

}

void dev_disc_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[dev_disc_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req || !req->params) {
        std::cerr << "[dev_disc_handle] Invalid request: params is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    if (!req->seq.empty()) {
    set_latest_join_seq(req->seq);
    std::cout << "[dev_disc_handle] Set latest join seq: " << req->seq << std::endl;
    }

    auto& doc = *req->params;
    // 解析 type
    if (!doc.HasMember("type") || !doc["type"].IsInt()) {
        std::cerr << "[dev_disc_handle] Missing or invalid 'type'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    int type = doc["type"].GetInt();


    //解析 duration
    if (!doc.HasMember("duration") || !doc["duration"].IsInt()) {
        std::cerr << "[dev_disc_handle] Missing or invalid 'duration'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }   
    int duration = doc["duration"].GetInt();
    
    result = oltiot_dev_disc(type, duration);
    // ====================== 发送响应 ======================
    oltiot_ack_resp(req, result);
}

void dev_stop_disc_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[dev_stop_disc_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req) {
        std::cerr << "[dev_stop_disc_handle] Invalid request: params is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    result = oltiot_dev_stop_disc();
    // ====================== 发送响应 ======================
    oltiot_ack_resp(req, result);
}

void dev_addsub_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[dev_addsub_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;
    std::vector< std::string > did_list;

    // 参数校验
    if (!req || !req->params) {
        std::cerr << "[dev_addsub_handle] Invalid request: params is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    if (!req->seq.empty()) {
    set_latest_join_seq(req->seq);
    std::cout << "[dev_addsub_handle] Set latest join seq: " << req->seq << std::endl;
    }

    auto& doc = *req->params;

    if (!doc.HasMember("devices") || !doc["devices"].IsArray()) {
        std::cerr << "[dev_addsub_handle] Missing or invalid 'devices' array" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    //解析 duration
    if (!doc.HasMember("duration") || !doc["duration"].IsInt()) {
        std::cerr << "[dev_addsub_handle] Missing or invalid 'duration'" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    int duration = doc["duration"].GetInt();

    const auto& devices = doc["devices"].GetArray();
    for (const auto& item : devices) 
    {
        if (!item.IsString()) {
            std::cerr << "[dev_addsub_handle] Invalid device entry (not string)" << std::endl;
            continue;
        }

        std::string did = item.GetString();
        std::cout << "[dev_addsub_handle] device = " << did << std::endl;
        did_list.push_back(did);//从json中解析出设备did并存入列表
    }

    result = oltiot_dev_add_sub(did_list, duration);

    // ====================== 发送响应 ======================
    oltiot_ack_resp(req, result);
}

void dev_delsub_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[dev_delsub_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req || !req->params) {
        std::cerr << "[dev_delsub_handle] Invalid request: params is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    auto& doc = *req->params;

    if (!doc.HasMember("devices") || !doc["devices"].IsArray()) {
        std::cerr << "[dev_delsub_handle] Missing or invalid 'devices' array" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }
    
    const auto& devices = doc["devices"].GetArray();
    for (const auto& item : devices) 
    {
        if (!item.IsString()) {
            std::cerr << "[dev_delsub_handle] Invalid device entry (not string)" << std::endl;
            continue;
        }

        std::string did = item.GetString();
        std::cout << "[dev_delsub_handle] device = " << did << std::endl;
        result = oltiot_dev_del_sub(did);
    }

    // ====================== 发送响应 ======================
    oltiot_ack_resp(req, result);
}

void dev_del_allsub_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[dev_del_allsub_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req) {
        std::cerr << "[dev_del_allsub_handle] Invalid request: req is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    result = oltiot_dev_del_allsub();
    // ====================== 发送响应 ======================
    oltiot_ack_resp(req, result);
}

void get_time_stamp_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[get_time_stamp_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req|| !req->params) {
        std::cerr << "[get_time_stamp_handle] Invalid request: params is null" << std::endl;
        return;
    }

    auto& doc = *req->params;

    //解析 timestamp
    if (!doc.HasMember("timestamp") || !doc["timestamp"].IsInt64()) {
        std::cerr << "[get_time_stamp_handle] Missing or invalid 'timestamp'" << std::endl;
        return;
    }
    long long timestamp = doc["timestamp"].GetInt64();



    oltiot_dev_gettime(timestamp);
    // ====================== 发送响应 ======================
    
}

void read_sublist_handle(const oltiot_msg_req_t* req, void* arg) {
    using namespace rapidjson;
    std::cout << "[read_sublist_handle] Callback called for method: " << req->method
              << ", topic: " << req->topic << std::endl;

    int result = OLTIOT_COMM_SUCCESS;

    // 参数校验
    if (!req) {
        std::cerr << "[read_sublist_handle] Invalid request: req is null" << std::endl;
        result = OLTIOT_COMM_PARM_ERROR;
        oltiot_ack_resp(req, result);
        return;
    }

    // 创建文档
    auto resp_params = std::make_shared<Document>();
    resp_params->SetObject();
    auto& alloc = resp_params->GetAllocator();

    Value devs_arr(kArrayType);

    // 获取设备数据
    auto dev_list = get_all_sub_dev_info();

    for (const auto& d : dev_list) {
        Value dev(kObjectType);

        dev.AddMember("did",          Value(d.did.c_str(), alloc), alloc);
        dev.AddMember("productModel", d.productModel, alloc);
        dev.AddMember("profileId",    d.profileId, alloc);
        dev.AddMember("mcu",          Value(d.mcu.c_str(), alloc), alloc);
        dev.AddMember("productType",  Value(d.productType.c_str(), alloc), alloc);
        dev.AddMember("powerType",    d.powerType, alloc);
        dev.AddMember("connectType",  d.connectType, alloc);
        dev.AddMember("heartBeat",    d.heartbeat, alloc);

        devs_arr.PushBack(dev, alloc);
    }

    resp_params->AddMember("devices", devs_arr, alloc);

    // ====================== 发送响应 ======================
    oltiot_msg_resp_t to_resp;
    oltiot_msg_resp_new(req, &to_resp);
    
    to_resp.topic  = "olt/receiver/" + req->src; 
    to_resp.result = result;
    to_resp.params = resp_params; 

    oltiot_msg_resp(&to_resp);
    
}


// ===== 注册示例函数 =====
void oltiot_devobj_register() {
    std::cout << "[oltiot_devobj_register] Registering OLTIOT device object handlers..." << std::endl;

    oltiot_msg_reg_t handle_reg;

    // // 设备禁用
    // memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    // handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    // handle_reg.method = "Dev.Block";
    // oltiot_msg_handle_reg(&handle_reg, default_ack_cb, nullptr);

    // // 设备禁用解除
    // memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    // handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    // handle_reg.method = "Dev.UnBlock";
    // oltiot_msg_handle_reg(&handle_reg, default_ack_cb, nullptr);

    // 读取设备属性
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.ReadPids";
    oltiot_msg_handle_reg(&handle_reg, readpids_handle, nullptr);

    // 写入设备属性
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.WritePids";
    oltiot_msg_handle_reg(&handle_reg, writepids_handle, nullptr);

    // 执行功能方法
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.DoFids";
    oltiot_msg_handle_reg(&handle_reg, dofids_handle, nullptr);

    // 让网关进入自动搜索子设备状态
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.Disc";
    oltiot_msg_handle_reg(&handle_reg, dev_disc_handle, nullptr);

    // 让网关停止搜索子设备
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.StopDisc";
    oltiot_msg_handle_reg(&handle_reg, dev_stop_disc_handle, nullptr);

    // 平台向网关确认添加子设备
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.AddSub";
    oltiot_msg_handle_reg(&handle_reg, dev_addsub_handle, nullptr);

    // 应用向网关发送删除子设备
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.DelSub";
    oltiot_msg_handle_reg(&handle_reg, dev_delsub_handle, nullptr);

    // 平台向网关发送全量读取子设备信息
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.ReadSubList";
    oltiot_msg_handle_reg(&handle_reg, read_sublist_handle, nullptr);

    // 应用向网关发送删除所有子设备
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.DelAll";
    oltiot_msg_handle_reg(&handle_reg, dev_del_allsub_handle, nullptr);

    // 获取网络时间
    memset(&handle_reg, 0, sizeof(oltiot_msg_reg_t));
    handle_reg.topic = "olt/receiver/" + oltiot_devobj_get_did();
    handle_reg.method = "Dev.GetTime";
    oltiot_msg_handle_reg(&handle_reg, get_time_stamp_handle, nullptr);

    std::cout << "[oltiot_devobj_register] All handlers registered." << std::endl;
}

int oltiot_get_time(oltiot_msg_req_t& req)
{
    if (req.method.empty())  req.method = "Dev.GetTime";
    if (req.topic.empty())   req.topic  = "olt/receiver/" + oltiot_devobj_get_did();
    if (req.src.empty())     req.src    = oltiot_devobj_get_did();
    if (req.dst.empty())     req.dst    = "0001000000000000";
    if (req.ver.empty())     req.ver    = "V1.0";
    if (req.seq.empty())     req.seq    = generate_seq();
    req.params = nullptr;    // 没有参数

    return oltiot_comm_send_guaranteed(req);
}

int oltiot_gateway_reg(const gateway_base_info_t& properties)
{
    using namespace rapidjson;

    if (properties.did.empty()) 
    {
        std::cerr << "[oltiot_gateway_reg] properties is empty, skip sending" << std::endl;
        return OLTIOT_COMM_FAILURE; 
    }

    oltiot_msg_req_t req= {};
    req.method = "Dev.Reg";
    req.topic  = "olt/receiver/0001000000000000";
    req.src    = oltiot_devobj_get_did();
    req.dst    = "0001000000000000";
    req.ver    = "V1.0";
    req.seq    = generate_seq();


    req.params = std::make_shared<Document>();
    auto& doc = *req.params;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    doc.AddMember("did", Value(properties.did.c_str(), alloc), alloc);
    doc.AddMember("productModel", Value(properties.productModel.c_str(), alloc), alloc);
    doc.AddMember("profileId", properties.profileId, alloc);
    doc.AddMember("mcu", Value(properties.mcu.c_str(), alloc), alloc);
    doc.AddMember("productType", Value(properties.productType.c_str(), alloc), alloc);
  

    // 调用发送接口
    return oltiot_comm_send_guaranteed(req);
}

int oltiot_report_pids(const property_item_t& prop)
{
    using namespace rapidjson;

    // ✅ 如果没有 pids，直接返回错误
    if (prop.pids.empty()) {
        std::cerr << "[oltiot_report_pids] pids is empty, skip sending" << std::endl;
        return OLTIOT_COMM_FAILURE;
    }

    oltiot_msg_req_t req{};
    req.method = "Dev.ReportPids";
    req.src    = oltiot_devobj_get_did();
    req.dst    = "0001000000000000";
    req.ver    = "V1.0";
    req.seq    = generate_seq();

    // === 构造 params 文档 ===
    req.params = std::make_shared<Document>();
    auto& doc = *req.params;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();

    // properties 对象 (不是数组)
    Value properties_obj(kObjectType);

    // did
    properties_obj.AddMember("did", Value(prop.did.c_str(), allocator), allocator);

    // pids 数组
    Value pids_arr(kArrayType);
    for (const auto& pid : prop.pids) {
        Value pid_obj(kObjectType);
        pid_obj.AddMember("sid", pid.sid, allocator);
        pid_obj.AddMember("pid", pid.pid, allocator);

        switch (pid.val.type) {
            case VALUE_TYPE_INT: {
                int int_val = 0;
                int_val = std::stoi(pid.val.value); // 假设整数存 string
                pid_obj.AddMember("val", int_val, allocator);
                break;
            }
            case VALUE_TYPE_STRING: {
                pid_obj.AddMember("val", Value(pid.val.value.c_str(), allocator), allocator);
                break;
            }
        }

        pids_arr.PushBack(pid_obj, allocator);
    }
    properties_obj.AddMember("pids", pids_arr, allocator);

    // 加入 params
    doc.AddMember("properties", properties_obj, allocator);

    req.topic  = "olt/report/pid/" + prop.did;
    // 发送
    return oltiot_comm_send_guaranteed(req);
}


int oltiot_report_eids(const eid_item_t& eids)
{
    using namespace rapidjson;

    if (eids.did.empty()) 
    {
        std::cerr << "[oltiot_report_eids] eids is empty, skip sending" << std::endl;
        return OLTIOT_COMM_FAILURE; 
    }

    oltiot_msg_req_t req= {};
    req.method = "Dev.ReportEids";
    req.src    = oltiot_devobj_get_did();
    req.dst    = "0003000000000000";
    req.ver    = "V1.0";
    req.seq    = generate_seq();

    // 创建 params 文档
    req.params = std::make_shared<Document>();
    auto& doc = *req.params;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();

    doc.AddMember("did", Value(eids.did.c_str(), allocator), allocator);
    doc.AddMember("sid", eids.sid, allocator);
    doc.AddMember("eid", eids.eid, allocator);
    doc.AddMember("val", eids.val, allocator);

    req.topic  = "olt/report/eid/" + eids.did + "/" + std::to_string(eids.eid);

    // 调用底层发送函数
    return oltiot_comm_send_guaranteed(req);
}

int oltiot_report_dev(const std::vector<dev_item_t>& devices)
{
    using namespace rapidjson;

    if (devices.empty()) 
    {
        std::cerr << "[oltiot_report_dev] devices is empty, skip sending" << std::endl;
        return OLTIOT_COMM_FAILURE; 
    }

    oltiot_msg_req_t req= {};
    req.method = "Dev.ReportDevices";
    req.topic  = "olt/receiver/0001000000000000";
    req.src    = oltiot_devobj_get_did();
    req.dst    = "0001000000000000";
    req.ver    = "V1.0";
    req.seq    = get_latest_join_seq();

    // 创建 params 文档
    req.params = std::make_shared<Document>();
    auto& doc = *req.params;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    // 构造 devices 数组
    Value devices_arr(kArrayType);

    for (const auto& dev : devices) {
        Value dev_obj(kObjectType);
        dev_obj.AddMember("did", Value(dev.did.c_str(), alloc), alloc);
        dev_obj.AddMember("productModel", dev.productModel, alloc);
        dev_obj.AddMember("profileId", dev.profileId, alloc);
        dev_obj.AddMember("mcu", Value(dev.mcu.c_str(), alloc), alloc);
        dev_obj.AddMember("productType", Value(dev.productType.c_str(), alloc), alloc);
        dev_obj.AddMember("powerType", dev.powerType, alloc);
        dev_obj.AddMember("connectType", dev.connectType, alloc);
        dev_obj.AddMember("heartBeat", dev.heartbeat, alloc);
        devices_arr.PushBack(dev_obj, alloc);
    }

    doc.AddMember("devices", devices_arr, alloc);
    
    // 调用底层 MQTT 发送
    return oltiot_comm_send_guaranteed(req);
}


int oltiot_report_online(const std::vector<online_item_t>& devices)
{
    using namespace rapidjson;

    if (devices.empty()) 
    {
        std::cerr << "[oltiot_report_online] devices is empty, skip sending" << std::endl;
        return OLTIOT_COMM_FAILURE; 
    }

    oltiot_msg_req_t req= {};
    req.method = "Dev.ReportOnline";
    req.topic  = "olt/receiver/0001000000000000";
    req.src    = oltiot_devobj_get_did();
    req.dst    = "0001000000000000";
    req.ver    = "V1.0";
    req.seq    = generate_seq();

    // === 创建 params 文档 ===
    req.params = std::make_shared<Document>();
    auto& doc = *req.params;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    // === 构造 devices 数组 ===
    Value devices_arr(kArrayType);

    for (const auto& dev : devices) {
        Value dev_obj(kObjectType);
        dev_obj.AddMember("did", Value(dev.did.c_str(), alloc), alloc);
        dev_obj.AddMember("online", dev.online ? 1 : 0, alloc);
        devices_arr.PushBack(dev_obj, alloc);
    }

    doc.AddMember("devices", devices_arr, alloc);

    // === 发送 ===
    return oltiot_comm_send_guaranteed(req);
}


int oltiot_report_del_dev(const std::vector<did_item_t>& devices)
{
    using namespace rapidjson;

    if (devices.empty()) 
    {
        std::cerr << "[oltiot_report_del_dev] devices is empty, skip sending" << std::endl;
        return OLTIOT_COMM_FAILURE; 
    }

    oltiot_msg_req_t req= {};
    req.method = "Dev.DelDevices";
    req.topic  = "olt/receiver/0001000000000000";
    req.src    = oltiot_devobj_get_did();
    req.dst    = "0001000000000000";
    req.ver    = "V1.0";
    req.seq    = generate_seq();

    // === 创建 params 文档 ===
    req.params = std::make_shared<Document>();
    auto& doc = *req.params;
    doc.SetObject();
    auto& alloc = doc.GetAllocator();

    // === 构造 devices 数组（字符串数组） ===
    Value devices_arr(kArrayType);

    for (const auto& dev : devices) {
        devices_arr.PushBack(Value(dev.did.c_str(), alloc), alloc);
    }

    doc.AddMember("devices", devices_arr, alloc);

    // === 发送 ===
    return oltiot_comm_send_guaranteed(req);
}

int oltiot_read_pids(const std::string& did, int sid, int pid, value_entry_t& out_value)
{
    // 读取成功
    out_value.type = VALUE_TYPE_INT;
    out_value.value = "12345";

    return OLTIOT_COMM_SUCCESS; 
}

int oltiot_write_pids(std::string did, int sid, int pid, const value_entry_t& val)
{
    std::cout << "[oltiot_write_pids] DID=" << did
              << " SID=" << sid
              << " PID=" << pid
              << " TYPE=" << val.type
              << " VALUE=" << val.value << std::endl;
    return OLTIOT_COMM_SUCCESS;
}

int oltiot_do_fids(std::string did, int sid, int fid, int val)
{
    std::cout << "[oltiot_do_fids] Do FID: DID=" << did
              << " SID=" << sid
              << " FID=" << fid
              << " VAL=" << val << std::endl;
    return OLTIOT_COMM_SUCCESS;
}

int oltiot_dev_disc(int type, int duration)
{
    std::cout << "[oltiot_dev_disc] Do Disc: Type=" << type
              << " Duration=" << duration << std::endl;
    return OLTIOT_COMM_SUCCESS;
}

int oltiot_dev_stop_disc()
{
    return OLTIOT_COMM_SUCCESS;
}

int oltiot_dev_add_sub(std::vector< std::string > did_list, int duration)
{
    std::cout << " Duration=" << duration << std::endl;

    for (const auto& did : did_list)
    {
        std::cout << "DID=" << did << std::endl;
    }
    return OLTIOT_COMM_SUCCESS;
}

int oltiot_dev_del_sub(std::string did)
{
    std::cout << "[oltiot_dev_del_sub] Delete Sub: DID=" << did << std::endl;
    return OLTIOT_COMM_SUCCESS;
}

int oltiot_dev_del_allsub()
{
    std::cout << "[oltiot_dev_del_allsub] Delete All Sub" << std::endl;
    return OLTIOT_COMM_SUCCESS;
}

int oltiot_dev_gettime(long long timestamp)
{
    std::cout << "[oltiot_dev_gettime] Get Time: Timestamp=" << timestamp << std::endl;
    return OLTIOT_COMM_SUCCESS;
}

std::vector<dev_item_t> get_all_sub_dev_info()
{ 
    std::vector<dev_item_t> devices;
    dev_item_t dev;
    dev.did = "011125092403004F";
    dev.productModel = 0x02;
    dev.profileId    = 0x02;
    dev.mcu          = "1:1.0";
    dev.productType  = "0x11";
    dev.powerType    = 1;
    dev.connectType  = 0;
    dev.heartbeat    = 28800;

    devices.push_back(dev);

    return {};
}

std::string oltiot_devobj_get_did()
{ 
    return "011025092402002F";
}