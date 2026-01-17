#include "../include/query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iomanip>

bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        }
    }
    
    if (r_name.empty() || start_date.empty() || end_date.empty() || num_threads <= 0 || table_path.empty() || result_path.empty()) {
        return false;
    }
    
    return true;
}

std::vector<std::map<std::string, std::string>> readCSV(const std::string& filepath, const std::vector<std::string>& columns, char delimiter = '|') {
    std::vector<std::map<std::string, std::string>> data;
    std::ifstream file(filepath);
    
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << filepath << std::endl;
        return data;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line.back() == delimiter) {
            if (!line.empty() && line.back() == delimiter) {
                line.pop_back();
            }
        }
        
        std::stringstream ss(line);
        std::string value;
        std::map<std::string, std::string> row;
        
        size_t col_idx = 0;
        while (std::getline(ss, value, delimiter) && col_idx < columns.size()) {
            row[columns[col_idx++]] = value;
        }
        
        if (!row.empty() && row.size() == columns.size()) {
            data.push_back(row);
        }
    }
    
    file.close();
    return data;
}

bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    
    std::cout << "Reading TPCH data from: " << table_path << std::endl;
    
    region_data = readCSV(table_path + "\\region.tbl", {"r_regionkey", "r_name", "r_comment"});
    std::cout << "Loaded " << region_data.size() << " regions" << std::endl;
    
    nation_data = readCSV(table_path + "\\nation.tbl", {"n_nationkey", "n_name", "n_regionkey", "n_comment"});
    std::cout << "Loaded " << nation_data.size() << " nations" << std::endl;
    
    supplier_data = readCSV(table_path + "\\supplier.tbl", {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"});
    std::cout << "Loaded " << supplier_data.size() << " suppliers" << std::endl;
    
    customer_data = readCSV(table_path + "\\customer.tbl", {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"});
    std::cout << "Loaded " << customer_data.size() << " customers" << std::endl;
    
    orders_data = readCSV(table_path + "\\orders.tbl", {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"});
    std::cout << "Loaded " << orders_data.size() << " orders" << std::endl;
    
    lineitem_data = readCSV(table_path + "\\lineitem.tbl", {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"});
    std::cout << "Loaded " << lineitem_data.size() << " line items" << std::endl;
    
    return !region_data.empty() && !nation_data.empty() && !supplier_data.empty() && !customer_data.empty() && !orders_data.empty() && !lineitem_data.empty();
}

bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    
    std::cout << "Executing Query 5..." << std::endl;
    std::cout << "Note: Running in single-threaded mode (num_threads parameter ignored)" << std::endl;
    
    std::string target_region_key;
    for (const auto& region : region_data) {
        if (region.at("r_name") == r_name) {
            target_region_key = region.at("r_regionkey");
            break;
        }
    }
    
    if (target_region_key.empty()) {
        std::cerr << "Region not found: " << r_name << std::endl;
        return false;
    }
    
    std::unordered_map<std::string, std::string> nation_names;
    std::unordered_set<std::string> valid_nations;
    
    for (const auto& nation : nation_data) {
        if (nation.at("n_regionkey") == target_region_key) {
            std::string n_key = nation.at("n_nationkey");
            nation_names[n_key] = nation.at("n_name");
            valid_nations.insert(n_key);
        }
    }
    
    std::unordered_map<std::string, std::string> supplier_to_nation;
    for (const auto& supplier : supplier_data) {
        std::string s_nationkey = supplier.at("s_nationkey");
        if (valid_nations.count(s_nationkey)) {
            supplier_to_nation[supplier.at("s_suppkey")] = s_nationkey;
        }
    }
    
    std::unordered_map<std::string, std::string> customer_to_nation;
    for (const auto& customer : customer_data) {
        std::string c_nationkey = customer.at("c_nationkey");
        if (valid_nations.count(c_nationkey)) {
            customer_to_nation[customer.at("c_custkey")] = c_nationkey;
        }
    }
    
    std::unordered_map<std::string, std::string> valid_orders;
    for (const auto& order : orders_data) {
        std::string order_date = order.at("o_orderdate");
        
        if (order_date >= start_date && order_date < end_date) {
            std::string custkey = order.at("o_custkey");
            auto cust_it = customer_to_nation.find(custkey);
            
            if (cust_it != customer_to_nation.end()) {
                valid_orders[order.at("o_orderkey")] = cust_it->second;
            }
        }
    }
    
    std::cout << "Valid orders: " << valid_orders.size() << std::endl;
    
    for (const auto& line : lineitem_data) {
        std::string l_orderkey = line.at("l_orderkey");
        std::string l_suppkey = line.at("l_suppkey");
        
        auto order_it = valid_orders.find(l_orderkey);
        if (order_it == valid_orders.end()) continue;
        
        std::string customer_nation = order_it->second;
        
        auto supp_it = supplier_to_nation.find(l_suppkey);
        if (supp_it == supplier_to_nation.end()) continue;
        
        std::string supplier_nation = supp_it->second;
        if (customer_nation != supplier_nation) continue;
        
        auto nation_it = nation_names.find(supplier_nation);
        if (nation_it == nation_names.end()) continue;
        
        std::string nation_name = nation_it->second;
        
        double extended_price = std::stod(line.at("l_extendedprice"));
        double discount = std::stod(line.at("l_discount"));
        double revenue = extended_price * (1.0 - discount);
        
        results[nation_name] += revenue;
    }
    
    std::cout << "Query execution completed. Found " << results.size() << " nations." << std::endl;
    return true;
}

bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream outfile(result_path);
    
    if (!outfile.is_open()) {
        std::cerr << "Error opening output file: " << result_path << std::endl;
        return false;
    }
    
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    
    std::sort(sorted_results.begin(), sorted_results.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
    
    outfile << "Nation Name|Revenue" << std::endl;
    
    for (const auto& pair : sorted_results) {
        outfile << pair.first << "|" << std::fixed << std::setprecision(2) << pair.second << std::endl;
        std::cout << pair.first << ": " << std::fixed << std::setprecision(2) << pair.second << std::endl;
    }
    
    outfile.close();
    std::cout << "Results written to: " << result_path << std::endl;
    return true;
}
