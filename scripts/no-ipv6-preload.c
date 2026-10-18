/* LD_PRELOAD shim that makes the kernel look like it was booted with
 * ipv6.disable=1: creating any AF_INET6 socket fails with EAFNOSUPPORT. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <sys/socket.h>

int socket(int domain, int type, int protocol) {
    static int (*real)(int, int, int);
    if (domain == AF_INET6) {
        errno = EAFNOSUPPORT;
        return -1;
    }
    if (!real)
        real = (int (*)(int, int, int))dlsym(RTLD_NEXT, "socket");
    return real(domain, type, protocol);
}
