#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace std;

// Helper function to parse CSV considering quotes
vector<string> parseCSV(const string& line) {
    vector<string> fields;
    string field;
    bool in_quotes = false;
    
    for (size_t i = 0; i < line.length(); i++) {
        char ch = line[i];
        if (ch == '"') {
            if (i + 1 < line.length() && line[i + 1] == '"') {
                field += '"';
                i++;
            } else {
                in_quotes = !in_quotes;
            }
        } else if (ch == ',' && !in_quotes) {
            fields.push_back(field);
            field.clear();
        } else {
            field += ch;
        }
    }
    
    fields.push_back(field);
    return fields;
}

int main(int argc, char* argv[]) {
    if (argc != 1) {
        cerr << "Usage: " << argv[0] << endl;
        return 1;
    }
    string input;
    char ch;
    while (cin.get(ch)) {
        input += ch;
    }
    if (input.empty()) {
        return 0;
    }
    
    size_t pos = 0;
    string delimiter = "]][";
    vector<string> tokens;
    
    if (input.substr(0, delimiter.length()) == delimiter) {
        input = input.substr(delimiter.length());
    }
    
    while ((pos = input.find(delimiter)) != string::npos) {
        if (pos == 0) {
            input = input.substr(delimiter.length());
            continue;
        }
        tokens.push_back(input.substr(0, pos));
        input = input.substr(pos + delimiter.length());
    }
    tokens.push_back(input);
    
    for (size_t i = 0; i < tokens.size(); i += 4) {
        if (i + 3 >= tokens.size()) break;
        string value = tokens[i + 2];
        if (value.length() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.length() - 2);
        }
        
        vector<string> fields = parseCSV(value);
        if (fields.size() >= 9) {
            string category = fields[8]; 
            cout << tokens[i] << "]][" << category << "]][" << "1" << "]][" << endl;
        }
    }
    
    return 0;
}