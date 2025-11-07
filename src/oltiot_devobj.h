#pragma once
#include "oltiot_comm.h"
#include <string>
#include <iostream>
#include <vector>

struct pid_item_t {
    int sid;
    int pid;
    int val;
};

struct property_item_t {
    std::string did;
    std::vector<pid_item_t> pids;
};

struct eid_item_t {
    std::string did;
    int sid;
    int eid;
    int val;
};

struct online_item_t {
    std::string did;
    bool online;   // true=在线, false=离线
};

struct did_item_t {
    std::string did;
};

struct gateway_base_info_t
{
    std::string did;
    std::string productModel;
    int profileId;
    std::string mcu;
    std::string productType;
};

struct dev_item_t {
    std::string did;
    std::string productModel;
    int profileId;
    std::string mcu;
    std::string productType;
    int powerType;
    int connectType;
    int sleepTime;
};

void oltiot_devobj_register();
int oltiot_send_message(const oltiot_msg_req_t& req);
int oltiot_get_time(oltiot_msg_req_t& req);
int oltiot_gateway_reg(const gateway_base_info_t& properties);
int oltiot_report_pids(const property_item_t& prop);
int oltiot_report_eids(const eid_item_t& eids);
int oltiot_report_dev(const std::vector<dev_item_t>& devices);
int oltiot_report_online(const std::vector<online_item_t>& devices);
int oltiot_report_del_dev(const std::vector<did_item_t>& devices);
std::string oltiot_devobj_get_did();