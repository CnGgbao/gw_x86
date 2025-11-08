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

using CommandExecutor = std::function<int(const std::vector<std::string>& args)>;

struct CommandEntry {
    std::string name;
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

    int cli_help_command(const std::vector<std::string>& args) {
        std::cout << "  C++ CMD LIST:" << std::endl;
        std::cout << "            help (h)" << std::endl;
        std::cout << "            info" << std::endl;
        std::cout << "            add_s <bid> <timeout>" << std::endl;
        std::cout << "            exit (q, quit)" << std::endl;
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

    int cli_test(const std::vector<std::string>& args) {
        std::cout << "C++ CLI Test Command Executed" << std::endl;
        std::cout << "Total arguments received: " << args.size() << std::endl;
        for(size_t i = 0; i < args.size(); ++i) {
            std::cout << "  arg[" << i << "]: " << args[i] << std::endl;
        }
        
        return 0;
    }
    
    std::vector<CommandEntry> g_command_list = {
        {"help",         cli_help_command},
        {"h",            cli_help_command},
        {"info",         cli_info_command},
        {"test",         cli_test},
    };

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