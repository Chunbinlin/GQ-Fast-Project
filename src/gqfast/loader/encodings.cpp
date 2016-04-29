#include <iostream>
#include "encodings.hpp"

Encodings::Encodings(string c, int e) {
        columnName = c;
        encoding = e;
}


string Encodings::getColumnName() {
        return columnName;
}

void Encodings::setColumnName(string c) {
        columnName = c;
}

int Encodings::getEncoding() {
        return encoding;
}

void Encodings::setEncoding(int e) {
        encoding = e;
}
