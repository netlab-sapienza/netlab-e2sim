#pragma once

// E2AP RICindicationSN is an unsigned 16-bit sequence, not an unbounded count.
// Normalize before incrementing so even LONG_MAX cannot overflow.
constexpr long indication_sequence(long value) {
    return static_cast<unsigned long>(value) & 0xffffUL;
}

constexpr long next_indication_sequence(long value) {
    return indication_sequence(indication_sequence(value) + 1);
}
