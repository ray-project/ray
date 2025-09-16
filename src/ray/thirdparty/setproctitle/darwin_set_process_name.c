/*

Set process title in a way compatible with Activity Monitor and other
MacOS system tools.

Idea is borrowed from libuv (used by node.js)
See https://github.com/libuv/libuv/blob/v1.x/src/unix/darwin-proctitle.c
Implementation rewritten from scratch, fixing various libuv bugs among other things

*/

#include <CoreFoundation/CoreFoundation.h>

#include <dlfcn.h>
#include <pthread.h>

#include "darwin_set_process_name.h"

#define DONE_IF(cond) if (cond) goto done;

/* Undocumented Launch Services functions */
typedef enum {
    kLSDefaultSessionID = -2,
} LSSessionID;
CFTypeRef LSGetCurrentApplicationASN(void);
OSStatus LSSetApplicationInformationItem(LSSessionID, CFTypeRef, CFStringRef, CFStringRef, CFDictionaryRef*);
CFDictionaryRef LSApplicationCheckIn(LSSessionID, CFDictionaryRef);
void LSSetApplicationLaunchServicesServerConnectionStatus(uint64_t, void *);

typedef struct {
    void * application_services_handle;

    CFBundleRef launch_services_bundle;
    typeof(LSGetCurrentApplicationASN) * pLSGetCurrentApplicationASN;
    typeof(LSSetApplicationInformationItem) * pLSSetApplicationInformationItem;
    typeof(LSApplicationCheckIn) * pLSApplicationCheckIn;
    typeof(LSSetApplicationLaunchServicesServerConnectionStatus) * pLSSetApplicationLaunchServicesServerConnectionStatus;

    CFStringRef * display_name_key_ptr;

} launch_services_t;

static bool launch_services_init(launch_services_t * it) {
    enum {
        has_nothing,
        has_application_services_handle
    } state = has_nothing;
    bool ret = false;

    it->application_services_handle = dlopen("/System/Library/Frameworks/"
                                             "ApplicationServices.framework/"
                                             "Versions/Current/ApplicationServices",
                                             RTLD_LAZY | RTLD_LOCAL);
    DONE_IF(!it->application_services_handle);
    ++state;
    
    it->launch_services_bundle = CFBundleGetBundleWithIdentifier(CFSTR("com.apple.LaunchServices"));
    DONE_IF(!it->launch_services_bundle);

#define LOAD_METHOD(name) \
    *(void **)(&it->p ## name ) = \
        CFBundleGetFunctionPointerForName(it->launch_services_bundle, CFSTR("_" #name)); \
    DONE_IF(!it->p ## name);

    LOAD_METHOD(LSGetCurrentApplicationASN)
    LOAD_METHOD(LSSetApplicationInformationItem)
    LOAD_METHOD(LSApplicationCheckIn)
    LOAD_METHOD(LSSetApplicationLaunchServicesServerConnectionStatus)

#undef LOAD_METHOD

    it->display_name_key_ptr = 
        CFBundleGetDataPointerForName(it->launch_services_bundle, CFSTR("_kLSDisplayNameKey"));
    DONE_IF(!it->display_name_key_ptr || !*it->display_name_key_ptr);

    ret = true;

done:
    switch(state) {
        case has_application_services_handle: if (!ret) dlclose(it->application_services_handle);
        case has_nothing: ;
    }
    return ret;
}

static inline void launch_services_destroy(launch_services_t * it) {
    dlclose(it->application_services_handle);
}

static bool launch_services_set_process_title(const launch_services_t * it, const char * title) {

    enum {
        has_nothing,
        has_cf_title
    } state = has_nothing;
    bool ret = false;
    
    static bool checked_in = false;

    CFTypeRef asn;
    CFStringRef cf_title;
    CFDictionaryRef info_dict;
    CFMutableDictionaryRef mutable_info_dict;
    CFStringRef LSUIElement_key;
    
    if (!checked_in) {
        it->pLSSetApplicationLaunchServicesServerConnectionStatus(0, NULL);

        // See https://github.com/dvarrazzo/py-setproctitle/issues/143
        // We need to set LSUIElement (https://developer.apple.com/documentation/bundleresources/information-property-list/lsuielement)
        // key to true to avoid macOS > 15 displaying the Dock icon.
        info_dict = CFBundleGetInfoDictionary(CFBundleGetMainBundle());
        mutable_info_dict = CFDictionaryCreateMutableCopy(NULL, 0, info_dict);
        LSUIElement_key = CFStringCreateWithCString(NULL, "LSUIElement", kCFStringEncodingUTF8);
        CFDictionaryAddValue(mutable_info_dict, LSUIElement_key, kCFBooleanTrue);
        CFRelease(LSUIElement_key);
        
        it->pLSApplicationCheckIn(kLSDefaultSessionID, mutable_info_dict);
        CFRelease(mutable_info_dict);
        
        checked_in = true;
    }

    asn = it->pLSGetCurrentApplicationASN();
    DONE_IF(!asn);
    
    cf_title = CFStringCreateWithCString(NULL, title, kCFStringEncodingUTF8);
    DONE_IF(!cf_title);
    ++state;
    DONE_IF(it->pLSSetApplicationInformationItem(kLSDefaultSessionID,
                                                 asn,
                                                 *it->display_name_key_ptr,
                                                 cf_title,
                                                 NULL) != noErr);
    ret = true;
done:
    switch(state) {
        case has_cf_title: CFRelease(cf_title);
        case has_nothing:  ;
    }

    return ret;
}

static bool darwin_pthread_setname_np(const char* name) {
    char namebuf[64];  /* MAXTHREADNAMESIZE according to libuv */
  
    strncpy(namebuf, name, sizeof(namebuf) - 1);
    namebuf[sizeof(namebuf) - 1] = '\0';

    return (pthread_setname_np(namebuf) != 0);
}


bool darwin_set_process_title(const char * title) {

    enum {
        has_nothing,
        has_launch_services
    } state = has_nothing;
    bool ret = false;

    launch_services_t launch_services;
    
    DONE_IF(!launch_services_init(&launch_services));
    ++state;

    DONE_IF(!launch_services_set_process_title(&launch_services, title));

    (void)darwin_pthread_setname_np(title); 

    ret = true;

done:
    switch(state) {
        case has_launch_services: launch_services_destroy(&launch_services);
        case has_nothing: ;
    }

    return ret;
}
