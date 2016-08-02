#ifndef dictionary__
#define dictionary__

#include <cmath>
#include <boost/serialization/array.hpp>
#include <boost/serialization/version.hpp>
#include <boost/cstdint.hpp>


class dictionary
{


public:

    uint32_t bits_info[2];
    uint64_t domain;
    uint64_t offset;

    dictionary()
    {
        domain = 1;
        offset = 0;
        bits_info[0] = 0;
        bits_info[1] = 0;
    }

    dictionary(int d, int o) : domain(d), offset(o)
    {

        // # of bits of integer "max"
        bits_info[0] = (uint32_t) floor(log2((double)domain)) + 1;

        // To extract bits
        bits_info[1] = pow(2,bits_info[0])-1;

    }

    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {

        ar & domain;
        ar & offset;
        ar & bits_info[0];
        ar & bits_info[1];
    }


};

#endif
