#include "oltiot.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <string>
#include "oltiot_devobj.h"

std::atomic<bool> running(true);

void input_thread() {
    std::string cmd;
    while (running) {
        std::cin >> cmd;  // 阻塞等待输入
        if (cmd == "q") {
            std::cout << "收到 q 命令，执行退出操作..." << std::endl;
            // 🔧 在这里写你要执行的命令
                /*report dev test*/
            std::vector<dev_item_t> devs ={{"0120030533060707", "WG001", 1, "1.0.0", "0x10", 1 , 1, 28800}};
            // std::vector<dev_item_t> devs ={{"0120030533060707", "WG001", 1, "1.0.0", "0x10", 1 , 1, 28800},{"0120030633060707", "WG001", 1, "1.0.0", "0x10", 1 , 1, 28800}};
            oltiot_report_dev(devs);
            running = false;
        } else {
            std::cout << "收到命令: " << cmd << std::endl;
            // 可以在这里加别的命令逻辑
        }
    }
}

int main()
{
    std::thread t(input_thread); // 开输入监听线程

    oltiot_init();
    g_retry_manager.start(); // ✅ 启动重传管理器线程
    std::cout << "\n✅ Running... (press Ctrl+C to exit)" << std::endl;
    
    // 主线程保持运行，用于维持 MQTT 长连接
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    t.join();
    return 0;
}
