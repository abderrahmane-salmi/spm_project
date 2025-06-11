#include <ff/ff.hpp>
#include <iostream>

using namespace ff;

int main() {
    ff::ffTime(START_TIME);
    std::cout << "Hello from FastFlow!\n";
    ff::ffTime(STOP_TIME);
    std::cout << "Time: " << ff::ffTime(GET_TIME) << " ms\n";
    return 0;
}
