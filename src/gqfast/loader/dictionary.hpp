#ifndef dictionary__
#define dictionary__

#include <cmath>
#include <boost/serialization/array.hpp>
#include <boost/serialization/version.hpp>
#include <boost/cstdint.hpp>


class dictionary {


public:

	uint32_t bits_info[2];
	int max;


	dictionary() {
		max = 1;
	}

	dictionary(int m) : max(m) {

		// # of bits of integer "max" 		
		bits_info[0] = (uint32_t) floor(log2((double)max)) + 1;

		// To extract bits
		bits_info[1] = pow(2,bits_info[0])-1;
		
	}

	template<class Archive>
    void serialize(Archive & ar, const unsigned int version) {
       
        ar & max;
        ar & bits_info[0];
        ar & bits_info[1];
    }


};

#endif