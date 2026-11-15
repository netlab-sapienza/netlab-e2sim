#pragma once
#include <stdexcept>
#include <string>

// Unset preserves the legacy sleep-before-receive behavior exactly.
// An explicit period enables immediate receive and paced request starts.
struct ReportTiming {
  int period_ms;
  bool immediate_receive;
};

inline ReportTiming report_timing(const char *value) {
  if (!value) return {500, false};
  const std::string text(value);
  if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
    throw std::invalid_argument("XAPPBLOCK_E2_PERIOD_MS must be an integer in [10, 1000]");
  const long period = std::stol(text);
  if (period < 10 || period > 1000)
    throw std::invalid_argument("XAPPBLOCK_E2_PERIOD_MS outside [10, 1000]");
  return {static_cast<int>(period), true};
}
