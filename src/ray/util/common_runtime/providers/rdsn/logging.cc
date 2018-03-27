#include "logging.h"

CrLogger::CrLogger(int severity) : severity_(severity), has_logged_(false) {}

CrLogger::~CrLogger() {}

void CrLogger::write_string(const char *str, size_t sz) {}