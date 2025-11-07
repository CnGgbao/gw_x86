#include "oltiot.h"
#include <iostream>
#include <thread>
#include <chrono>


// const std::string SERVER_ADDRESS(OLTIOT_SERVE_URI);
// const std::string CLIENT_ID(OLT_CLIENT_ID);

int main()
{
    oltiot_init();

    std::cout << "\n✅ Running... (press Ctrl+C to exit)" << std::endl;
    
    // 主线程保持运行，用于维持 MQTT 长连接
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    return 0;
}
