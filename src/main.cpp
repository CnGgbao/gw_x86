#include "oltiot.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <string>
#include "oltiot_devobj.h"
#include "cli.h"

int main()
{
    oltiot_init();
    g_retry_manager.start(); // ✅ 启动重传管理器线程
    cli_init();
    cli_start(true);
    std::cout << "\n✅ Running... (press Ctrl+C to exit)" << std::endl;
    
    // 主线程保持运行，用于维持 MQTT 长连接
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    return 0;
}
