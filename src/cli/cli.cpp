#include "cli.h"

#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <atomic>
#include <sstream>
#include <algorithm>
#include <memory>
#include "oltiot_devobj.h"
#include "oltiot.h"

using CommandExecutor = std::function<int(const std::vector<std::string>& args)>;

struct CommandEntry {
    std::string name;
    std::string desc;
    std::string usage;
    CommandExecutor executor;
};

class CliFramework {
public:
    CliFramework(const std::vector<CommandEntry>& command_list) 
        : running_(false) 
    {
        std::lock_guard<std::mutex> lock(command_mutex_);
        
        for (const auto& entry : command_list) {
            if (entry.executor) {
                commands_[entry.name] = entry.executor;
            }
        }
        std::cout << "[CLI] C++ Framework initialized with " << commands_.size() << " commands." << std::endl;
    }

    ~CliFramework() {
        stop();
    }

    CliFramework(const CliFramework&) = delete;
    CliFramework& operator=(const CliFramework&) = delete;

    void start(bool background = true) {
        if (running_) {
            std::cerr << "CLI framework is already running." << std::endl;
            return;
        }
        
        running_ = true;

        if (background) {
            cli_thread_ = std::thread(&CliFramework::run, this);
        } else {
            run();
        }
    }

    void stop() {
        running_ = false;

        if (cli_thread_.joinable()) {
            cli_thread_.join();
        }
    }

private:
    std::string trim(const std::string& str) const {
        const char* whitespace = " \t\n\r\f\v";
        size_t first = str.find_first_not_of(whitespace);
        if (std::string::npos == first) {
            return "";
        }
        size_t last = str.find_last_not_of(whitespace);
        return str.substr(first, (last - first + 1));
    }

    void run() {
        std::string line;
        std::cout << "\n[CLI] C++ CLI Framework is active. Type 'help' or 'exit'." << std::endl;
        
        while (running_) {
            std::cout << "> " << std::flush;
            
            if (!std::getline(std::cin, line)) {
                running_ = false;
                break;
            }

            std::string trimmed_line = trim(line);

            if (trimmed_line.empty()) {
                continue;
            }

            if (trimmed_line == "q" || trimmed_line == "exit" || trimmed_line == "quit") {
                running_ = false;
                break;
            }

            execute_line(trimmed_line);
        }
        std::cout << "[CLI] Exiting CLI thread." << std::endl;
    }

    void execute_line(const std::string& line) {
        std::stringstream ss(line);
        std::string segment;
        std::vector<std::string> args;

        while (ss >> segment) {
            args.push_back(segment);
        }

        if (args.empty()) {
            return;
        }

        std::string command_name = args[0];
        CommandExecutor executor = nullptr;

        {
            auto it = commands_.find(command_name);
            if (it != commands_.end()) {
                executor = it->second; 
            }
        }

        if (executor) {
            try {
                executor(args);
            } catch (const std::exception& e) {
                std::cerr << "Error executing command '" << command_name << "': " << e.what() << std::endl;
            }
        } else {
            std::cerr << "Invalid command: " << command_name << std::endl;
        }
    }

    std::unordered_map<std::string, CommandExecutor> commands_;
    std::thread cli_thread_;
    std::atomic<bool> running_;
    std::mutex command_mutex_;
};


namespace {

    size_t get_visual_width(const std::string& str) {
        size_t width = 0;
        for (size_t i = 0; i < str.length(); ) {
            unsigned char c = str[i];
            if (c <= 0x7F) {
                // ASCII 字符 (1 字节, 1 栏)
                width += 1;
                i += 1;
            } else if ((c & 0xF0) == 0xE0) {
                // 3 字节 UTF-8 字符 (通常是中文，占 2 栏)
                width += 2;
                i += 3; // 跳过 3 字节
            } else {
                // 其他多字节字符 (例如 2 字节或 4 字节，这里简化处理为 1 栏)
                width += 1;
                i += 1; 
            }
        }
        return width;
    }

    // **在这里添加前置声明**
    int cli_help_command(const std::vector<std::string>& args);
    int cli_info_command(const std::vector<std::string>& args);
    int cli_list_commands(const std::vector<std::string>& args);
    int cli_test(const std::vector<std::string>& args);
    int cli_dev_sub_add(const std::vector<std::string>& args);
    int cli_dev_report_pids(const std::vector<std::string>& args);
    int cli_gateway_reg(const std::vector<std::string>& args);
    int cli_dev_report_eids(const std::vector<std::string>& args);
    int cli_dev_report_online(const std::vector<std::string>& args);
    int cli_dev_del_dev(const std::vector<std::string>& args);
    int show_mqtt_is_connected(const std::vector<std::string>& args);
    int cli_dev_report_upgrade_progress(const std::vector<std::string>& args);

    std::vector<CommandEntry> g_command_list = {
        // NAME                          DESCRIPTION               USAGE                                                                                           EXECUTOR
        {"help",                         "显示帮助信息",           "<none>",                                                                                               cli_help_command},
        {"h",                            "help 的简写",            "<none>",                                                                                               cli_help_command},
        {"info",                         "显示 CLI 框架信息",      "<none>",                                                                                               cli_info_command},
        {"list",                         "显示当前所有命令",       "<none>",                                                                                               cli_list_commands},
        {"test",                         "测试命令",               "<none>",                                                                                               cli_test},
        {"dev_sub_add",                  "添加子设备",             "<did> <productModel> <profileId> <mcu> <productType> <powerType> <connectType> <heartbeat>",           cli_dev_sub_add},
        {"dev_report_pids",              "上报 PID 属性",          "<did> <pid> <value>",                                                                                  cli_dev_report_pids},
        {"gateway_reg",                  "注册网关设备",           "<did> <productModel> <profileId> <mcu> <productType>",                                                 cli_gateway_reg},
        {"dev_report_eids",              "上报 EID 事件",          "<did> <sid> <eid> <val>",                                                                              cli_dev_report_eids},
        {"dev_report_online",            "上报设备在线状态",       "<did> <online(0|1)>",                                                                                  cli_dev_report_online},
        {"dev_del_dev",                  "删除子设备",             "<did>",                                                                                                cli_dev_del_dev},
        {"mqtt_is_connected",            "MQTT 连接状态",          "<none>",                                                                                               show_mqtt_is_connected},
        {"dev_report_upgrade_progress",  "上报升级进度",           "<did> <mcu> <step> <progress>",                                                                       cli_dev_report_upgrade_progress},
    };
    int cli_help_command(const std::vector<std::string>& args) {
    const int MIN_TOTAL_WIDTH = 80;   // 最小总宽
    const int DESC_COL_WIDTH = 40;    // DESCRIPTION 列宽
    const int USAGE_MAX_WIDTH = 50;   // USAGE 最大列宽

    // 计算 NAME 列宽：根据最长命令名动态计算
    int NAME_COL_WIDTH = 0;
    for (auto& e : g_command_list)
        NAME_COL_WIDTH = std::max(NAME_COL_WIDTH, (int)get_visual_width(e.name) + 2);

    // 辅助函数：按显示宽度填充空格
    auto pad_right = [](const std::string& text, int total_width) -> std::string {
        int width = (int)get_visual_width(text); // 中文算2，英文算1
        int pad_len = std::max(0, total_width - width);
        return text + std::string(pad_len, ' ');
    };

    // 顶部线
    int final_width = NAME_COL_WIDTH + DESC_COL_WIDTH + USAGE_MAX_WIDTH + 2; // 2 缩进
    final_width = std::max(final_width, MIN_TOTAL_WIDTH);
    std::string header_text = " CLI HELP ";
    size_t header_len = get_visual_width(header_text);
    int pad_len = std::max(1, (final_width - (int)header_len) / 2);
    std::string header_line = std::string(pad_len, '=') + header_text + std::string(final_width - pad_len - (int)header_len, '=');

    std::cout << header_line << std::endl;
    std::cout << "输入以下命令之一执行操作：" << std::endl;
    std::cout << "退出请输入 'exit' 或 'q'" << std::endl;
    std::cout << "\n可用命令:" << std::endl;

    // 表头
    std::cout << "  " << pad_right("NAME", NAME_COL_WIDTH)
              << pad_right("DESCRIPTION", DESC_COL_WIDTH)
              << "USAGE" << std::endl;

    // 分隔线
    std::cout << std::string(final_width, '-') << std::endl;

    // 打印命令表
    for (const auto& entry : g_command_list) {
        std::string usage = entry.usage;
        size_t pos = 0;
        bool first_line = true;

        while (pos < usage.size()) {
            std::string part = usage.substr(pos, USAGE_MAX_WIDTH);
            
            if (first_line) {
                // 首行：打印 NAME + DESC + USAGE
                std::cout << "  " << pad_right(entry.name, NAME_COL_WIDTH)
                          << pad_right(entry.desc, DESC_COL_WIDTH)
                          << part << std::endl;
                first_line = false;
            } else {
                // 后续行只打印缩进，保持列对齐
                std::cout << std::string(NAME_COL_WIDTH + DESC_COL_WIDTH + 2, ' ')
                          << part << std::endl;
            }
            pos += USAGE_MAX_WIDTH;
        }

        // 如果 usage 没超长也打印一次首行
        if (first_line) {
            std::cout << "  " << pad_right(entry.name, NAME_COL_WIDTH)
                      << pad_right(entry.desc, DESC_COL_WIDTH)
                      << usage << std::endl;
        }
    }

    std::cout << header_line << std::endl;
    return 0;
}


    int cli_info_command(const std::vector<std::string>& args) {
        std::cout << "C++ CLI Framework Info" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        for(size_t i = 0; i < args.size(); ++i) {
            std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        }
        return 0;
    }

    int cli_list_commands(const std::vector<std::string>& args) {
        // 定义每列分配的固定宽度 (栏数)
        const int NAME_COL_WIDTH = 20;
        const int DESC_COL_WIDTH = 25; 
        const int MIN_TOTAL_WIDTH = 50; // 最小宽度

        // 1. 计算最大所需宽度
        int max_content_width = 0;
        
        // 计算表头宽度
        int header_width = 2 + NAME_COL_WIDTH + DESC_COL_WIDTH + (int)get_visual_width("USAGE");
        max_content_width = std::max(max_content_width, header_width);

        for (const auto& entry : g_command_list) {
            int current_row_width = 2 + NAME_COL_WIDTH + DESC_COL_WIDTH + (int)get_visual_width(entry.usage);
            if (current_row_width > max_content_width) {
                max_content_width = current_row_width;
            }
        }
        
        int final_width = std::max(max_content_width, MIN_TOTAL_WIDTH);

        std::cout << "[CLI] 当前可用命令：" << std::endl;

        // 打印表头: NAME, DESCRIPTION, USAGE
        std::cout << "  NAME" << std::string(NAME_COL_WIDTH - 4, ' ') 
                  << "DESCRIPTION" << std::string(DESC_COL_WIDTH - 11, ' ') 
                  << "USAGE" << std::endl;
                  
        // 打印分隔线
        std::cout << std::string(final_width, '-') << std::endl;

        for (const auto& entry : g_command_list) {
            size_t name_width = get_visual_width(entry.name);
            size_t desc_width = get_visual_width(entry.desc);
            
            // 1. 打印命令名 NAME (总宽 NAME_COL_WIDTH)
            std::cout << "  " << entry.name << std::string(std::max(1, NAME_COL_WIDTH - (int)name_width), ' ');
            
            // 2. 打印描述 DESCRIPTION (总宽 DESC_COL_WIDTH)
            std::cout << entry.desc << std::string(std::max(1, DESC_COL_WIDTH - (int)desc_width), ' ');
            
            // 3. 打印用法 USAGE
            std::cout << entry.usage;
            
            std::cout << std::endl;
        }
        return 0;
    }


    int cli_test(const std::vector<std::string>& args) {
        std::cout << "C++ CLI Test Command Executed" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        for(size_t i = 0; i < args.size(); ++i) {
            std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        }
        
        return 0;
    }
    
    int cli_dev_sub_add(const std::vector<std::string>& args) {
        std::cout << "cli_dev_sub_add" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        // for(size_t i = 0; i < args.size(); ++i) {
        //     std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        // }

        if (args.size() < 9) {
            std::cerr << "Usage: dev_sub_add <did> <productModel> <profileId> <mcu> <productType> <powerType> <connectType> <heartbeat>" << std::endl;
            return -1;
        }

        std::vector<dev_item_t> devs;
        dev_item_t item;
        item.did = args[1];
        item.productModel = std::stoi(args[2]);
        item.profileId = std::stoi(args[3]);
        item.mcu = args[4];
        item.productType = args[5];
        item.powerType = std::stoi(args[6]);
        item.connectType = std::stoi(args[7]);
        item.heartbeat = std::stoi(args[8]);

        std::cout <<"did         :" << item.did << std::endl;
        std::cout <<"productModel:" << item.productModel << std::endl;
        std::cout <<"profileId   :" << item.profileId << std::endl;
        std::cout <<"mcu         :" << item.mcu << std::endl;
        std::cout <<"productType :" << item.productType << std::endl;
        std::cout <<"powerType   :" << item.powerType << std::endl;
        std::cout <<"connectType :" << item.connectType << std::endl;
        std::cout <<"heartbeat   :" << item.heartbeat << std::endl;

        devs.push_back(item);
        oltiot_report_dev(devs);

        return 0;
    }

    int cli_dev_report_pids(const std::vector<std::string>& args) {
        std::cout << "cli_dev_report_pids" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        // for (size_t i = 0; i < args.size(); ++i) {
        //     std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        // }

        if (args.size() < 4) {
            std::cerr << "Usage: dev_report_pids <did> <pid> <value>" << std::endl;
            return -1;
        }

        std::string did = args[1];

        pid_item_t item;
        item.sid = 0;
        item.pid = std::stoi(args[2], nullptr, 16);
        item.val.type = VALUE_TYPE_INT;
        item.val.value = args[3];

        std::cout <<"did      :" << item.sid << std::endl;
        std::cout <<"pid      :0x" << std::hex << std::uppercase << item.pid  << std::endl;
        std::cout <<"value    :" << item.val.value << std::endl;

        std::vector<pid_item_t> pids;
        pids.push_back(item);

        property_item_t prop = {did, pids};
        oltiot_report_pids(prop);

        return 0;
    }

    int cli_gateway_reg(const std::vector<std::string>& args) {
        std::cout << "cli_gateway_reg" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        // for(size_t i = 0; i < args.size(); ++i) {
        //     std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        // }

        if (args.size() < 6) {
            std::cerr << "Usage: gateway_reg <did> <productModel> <profileId> <mcu> <productType>" << std::endl;
            return -1;
        }

        gateway_base_info_t info;
        info.did = args[1];
        info.productModel = args[2];
        info.profileId = std::stoi(args[3]);
        info.mcu = args[4];
        info.productType = args[5];

        std::cout <<"did         :" << info.did << std::endl;
        std::cout <<"productModel:" << info.productModel << std::endl;
        std::cout <<"profileId   :" << info.profileId << std::endl;
        std::cout <<"mcu         :" << info.mcu << std::endl;
        std::cout <<"productType :" << info.productType << std::endl;
        oltiot_gateway_reg(info);
        return 0;
    }

    int cli_dev_report_eids(const std::vector<std::string>& args) {
        std::cout << "cli_dev_report_eids" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        // for(size_t i = 0; i < args.size(); ++i) {
        //     std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        // }
        
        if (args.size() < 5) {
            std::cerr << "Usage: dev_report_eids <did> <sid> <eid> <val>" << std::endl;
            return -1;
        }

        eid_item_t eid;
        eid.did = args[1];
        eid.sid = std::stoi(args[2]);
        eid.eid = std::stoi(args[3], nullptr, 16);
        eid.val = std::stoi(args[4]);

        std::cout <<"did      :" << eid.did << std::endl;
        std::cout <<"sid      :" << eid.sid << std::endl;
        std::cout <<"eid      :0x" << std::hex << std::uppercase << eid.eid << std::endl;
        std::cout <<"val      :" << eid.val << std::endl;

        oltiot_report_eids(eid);
        return 0;
    }

    int cli_dev_report_online(const std::vector<std::string>& args) {
        std::cout << "cli_dev_report_online" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        for(size_t i = 0; i < args.size(); ++i) {
            std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        }

        if( args.size() < 3) {
            std::cerr << "Usage: dev_report_online <did> <online>" << std::endl;
            return -1;
        }


        std::vector<online_item_t> online_list;
        online_item_t online;
        online.did = args[1];
        online.online = (args[2] == "1") ? true : false;

        std::cout <<"did      :" << online.did << std::endl;
        std::cout <<"online   :" << online.online << std::endl;

        online_list.push_back(online);
        oltiot_report_online(online_list);
        return 0;
    }

    int cli_dev_del_dev(const std::vector<std::string>& args) {
        std::cout << "cli_dev_del_dev" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        // for(size_t i = 0; i < args.size(); ++i) {
        //     std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        // }

        if (args.size() < 2) {
            std::cerr << "Usage: dev_del_dev <did>" << std::endl;
            return -1;
        }

        std::vector<did_item_t> did_list;
        did_item_t did;
        did.did = args[1];

        std::cout <<"did      :" << did.did << std::endl;

        did_list.push_back(did);
        oltiot_report_del_dev(did_list);
        return 0;
    }

    int show_mqtt_is_connected(const std::vector<std::string>& args) {
        std::cout << "mqtt_is_connected" << std::endl;
        bool connected = mqtt_is_connected();
        std::cout << "MQTT Connected: " << (connected ? "Yes" : "No") << std::endl;
        return 0;
    }

    int cli_dev_report_upgrade_progress(const std::vector<std::string>& args) {
        std::cout << "cli_dev_report_upgrade_progress" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;

        if (args.size() < 5) {
            std::cerr << "Usage: dev_report_upgrade_progress <did> <mcu> <step> <progress>" << std::endl;
            return -1;
        }

        std::string did = args[1];
        std::string mcu = args[2];
        int step = std::stoi(args[3]);
        int progress = std::stoi(args[4]);

        std::cout <<"did      :" << did << std::endl;
        std::cout <<"mcu      :" << mcu << std::endl;
        std::cout <<"step     :" << step << std::endl;
        std::cout <<"progress :" << progress << std::endl;

        oltiot_report_upgrade_progress(did, mcu, step, progress);
        return 0;
    }

    std::unique_ptr<CliFramework> g_cli;

}

void cli_init() {
    if (g_cli) {
        return;
    }
    g_cli = std::unique_ptr<CliFramework>(new CliFramework(g_command_list));
}

void cli_start(bool background) {
    if (g_cli) {
        g_cli->start(background);
    } else {
        std::cerr << "Error: CLI not initialized. Call cli_init() first." << std::endl;
    }
}