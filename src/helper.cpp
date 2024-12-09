#include <string>
#include <vector>
#include <fstream>
#include <iostream>

std::vector<char> readFileIntoVector(const std::string& filename) {
 std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file) {
        std::cout << "Failed to open file: " << filename << "\n";
        return std::vector<char>();  // Return empty vector, no copy involved here
    }

    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    // i think rvo makes this ok
    std::vector<char> buffer(size);
    if (file.read(buffer.data(), size)) {
        return buffer;
    } else {
        std::cout << "Failed to read file: " << filename << "\n";
        return std::vector<char>();
    }
}