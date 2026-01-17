#include "../include/query5.hpp"
#include <iostream>
#include <chrono>

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads = 1;

    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        std::cerr << "Usage: " << argv[0] << " --r_name <region> --start_date <date> --end_date <date> --threads <num> --table_path <path> --result_path <path>" << std::endl;
        return 1;
    }

    std::vector<std::map<std::string, std::string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;

    std::cout << "Loading TPCH data..." << std::endl;
    auto start_load = std::chrono::high_resolution_clock::now();
    
    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        std::cerr << "Failed to read TPCH data" << std::endl;
        return 1;
    }
    
    auto end_load = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> load_duration = end_load - start_load;
    std::cout << "Data loading completed in " << load_duration.count() << " seconds" << std::endl;

    std::map<std::string, double> results;

    std::cout << "Executing Query 5 with " << num_threads << " threads..." << std::endl;
    auto start_query = std::chrono::high_resolution_clock::now();
    
    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        std::cerr << "Query execution failed" << std::endl;
        return 1;
    }
    
    auto end_query = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> query_duration = end_query - start_query;
    std::cout << "Query execution completed in " << query_duration.count() << " seconds" << std::endl;

    if (!outputResults(result_path, results)) {
        std::cerr << "Failed to write results" << std::endl;
        return 1;
    }

    std::cout << "Total runtime: " << (load_duration.count() + query_duration.count()) << " seconds" << std::endl;

    return 0;
}
